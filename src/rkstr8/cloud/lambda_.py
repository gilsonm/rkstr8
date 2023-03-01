from rkstr8.cloud.s3 import S3Upload
import logging
import os, sys
import shutil
import subprocess
import errno
import tempfile
from typing import List, Callable
from contextlib import contextmanager
from pathlib import Path
from collections import namedtuple
from rkstr8.cloud import Command
from rkstr8.cloud.cloudformation import TemplateProcessor
from rkstr8.conf import Config

PIP_INSTALL_REQUIREMENTS_TMPL = ' '.join((
    'pip install -r {requirements}',
    '-t {build_dir}'
))

PY_PACKAGE_NAME = '__init__.py'
REQS_SUFFIX = '_requirements.txt'

Deployment = namedtuple('Deployment', ['handler_path', 'bucket', 'key'])

HandlerLocal = namedtuple('HandlerLocal', ['handler_path', 'requirements_path'])

TemplateExport = namedtuple('TemplateExport', ['export_name', 'handler_module'])


class LambdaTemplateRendering(object):
    '''
    Entry-point for lambda template rendering
    '''

    def __init__(self, config: Config):
        self.lambda_config = config.lambda_config
        self.handler_deployments = []

    def handler_name_from_deployment(self, handler_deployment):
        return handler_deployment.handler_path.stem + '.handler'

    # TODO: this is duplicated in Batch
    def _to_camel(self, snake_string):
        return ''.join([p.capitalize() for p in snake_string.split('_')])

    def lambda_resource_block(self, handler_deployment):
        return {
            'Type': "AWS::Lambda::Function",
            'Properties': {
                'Handler': self.handler_name_from_deployment(handler_deployment),
                'Role': {
                    'Fn::GetAtt': [ 'BatchLambdaExecutionRole', 'Arn' ]
                },
                'Code': {
                    'S3Bucket': handler_deployment.bucket,
                    'S3Key': handler_deployment.key
                },
                'Runtime': "python3.9",
                'Timeout': self.lambda_config.time_out
            },
            'DependsOn': 'BatchLambdaExecutionRole'
        }

    def lambda_resources(self):
        return [self.lambda_resource_block(handler_deployment=handler_deployment)
                for handler_deployment in self.handler_deployments]

    def lambda_exports(self):
        return [self.lambda_export_block(resource_id=resource_id)
                for resource_id in self.resource_ids()]

    def lambda_export_block(self, resource_id):
        print('LAMBDA EXPORT CALL')
        return {
            'Export': {
                'Name': {
                    'Fn::Sub': '${StackUID}-' + resource_id
                }
            },
            'Value': {
                'Fn::GetAtt': [resource_id, 'Arn']
            }
        }

    def resource_ids(self):
        # return ['handler' + str(i) for i in range(len(self.handler_deployments))]
        return [self._to_camel(depl.handler_path.stem) for depl in self.handler_deployments]

    def _render_template(self, resolver):
        # Resources and exports bindings provide the maps referred to above
        template_base = resolver(self.lambda_config.template_base)
        lambda_template_pipe = TemplateProcessor([template_base]) \
            .from_yaml(as_path=True) \
            .add_resources(names=self.resource_ids(), definitions=self.lambda_resources()) \
            .add_exports(names=self.resource_ids(), definitions=self.lambda_exports()) \
            .to_yaml()

        return list(lambda_template_pipe)[0]

    def write_template_to_file(self, lambda_template, resolver):
        rendered_template_path = resolver(self.lambda_config.rendered_template_path)
        with open(rendered_template_path,'w') as fh:
            fh.write(lambda_template)

    def get_template_exports(self):
        return [
            TemplateExport(export_name=export_name, handler_module=handler_module)
            for export_name, handler_module in zip(
                ['${StackUID}-' + resource_id for resource_id in self.resource_ids()],
                [depl.handler_path.stem for depl in self.handler_deployments]
            )
        ]

    def render_and_write_template(self, handler_deployments: List[Deployment], resolver: Callable) -> None:
        # handler_i -> depl_i
        # handler_i -> output_i
        # dummy_output_token_i -x
        #
        # Need this map:
        # depl_i -> output_i
        #
        # Then, when rendering state machine subs, have the handler name (from depl_i)
        # and the output for handler_i (output_i) in the same scope. Use the handler name
        # to create the substitution token (mx_token_i / user_token_i) that the user
        # provides to bind a Lambda function to a task at pipeline definition time.
        self.handler_deployments = handler_deployments.copy()
        lambda_template = self._render_template(resolver)
        self.write_template_to_file(lambda_template, resolver)


class LambdaDiscovery(object):
    '''
    This class is responsible for parsing the <handlers> directory contents and returning a list of Lambda objects
    '''

    def __init__(self, config: Config):
        self.lambda_config = config.lambda_config
        # Constructor for matches between lambda handler py and requirements.txt
        self.Match = namedtuple('Match', ['handler','reqs'])

    def find_handlers(self, dir):
        return [handler_path for handler_path in Path(dir).glob('*.py') if
                PY_PACKAGE_NAME not in handler_path.parts]

    def find_requirements(self, dir):
        return [req_path for req_path in Path(dir).glob('*' + REQS_SUFFIX)]

    def match_handlers_and_requirements(self, resolver):
        handler_dir = resolver(self.lambda_config.handler_dir)
        handler_paths = self.find_handlers(handler_dir)
        requirements_paths = self.find_requirements(handler_dir)

        lambda_pairs = []

        for handler in handler_paths:
            matches = [req for req in requirements_paths if req.name == handler.stem + REQS_SUFFIX]
            try:
                match = matches.pop()
            except IndexError:
                # matches.pop() failed on empty list viz. no matches for this handler
                pass
            else:
                lambda_pairs.append(
                    self.Match(handler, match)
                )

        return lambda_pairs

    def handler_locals(self, resolver):
        return [
            HandlerLocal(
                handler_path=match.handler,
                requirements_path=match.reqs
            ) for match in self.match_handlers_and_requirements(resolver)
        ]


class LambdaDeployment(object):

    def __init__(self, config: Config):
        self.lambda_config = config.lambda_config
        self.handler_deployments = []

    def stage_deployments(self, handler_locals, rkstr8_wheel):

        rkstr8_wheel_path = rkstr8_wheel
        s3_dest_bucket = self.lambda_config.asset_bucket
        build_dir_base = self.lambda_config.build_dir_base

        for handler_path in [handler_local.handler_path for handler_local in handler_locals]:
            with deploy(build_dir_base, handler_path, rkstr8_wheel_path, s3_dest_bucket) as deployer:
                deployer.create_deployment_package()
                bucket, key = deployer.upload_deployment_package()
                self.handler_deployments.append(
                    Deployment(handler_path=handler_path, bucket=bucket, key=key)
                )

        return self.handler_deployments


@contextmanager
def deploy_mock(handler_path, rkstr8_path, s3_bucket):

    build_dir = '.%s' % (handler_path.stem)
    requirements_file = handler_path.parent / '{}_requirements.txt'.format(handler_path.stem)
    deployment_zip = '%s.deployable' % (handler_path.stem)
    deployment_bucket = s3_bucket
    deployment_key = '.'.join((deployment_zip, 'zip'))

    class MockDeploy(object):
        def __init__(self, zipf, buck, key):
            self.zipf = zipf
            self.buck = buck
            self.key = key
        def create_deployment_package(self):
            pass
        def upload_deployment_package(self):
            print('Uploading {} to s3://{}/{}'.format(self.zipf, self.buck, self.key))
            return self.buck, self.key

    yield MockDeploy(deployment_zip, deployment_bucket, deployment_key)

    pass


@contextmanager
def deploy(build_dir_base, handler_path, rkstr8_path, s3_bucket):

    requirements_file = handler_path.parent / '{}_requirements.txt'.format(handler_path.stem)
    deployment_zip = 'lambda_deployment'
    deployment_bucket = s3_bucket
    deployment_key = '.'.join((deployment_zip, 'zip'))

    builder = DeploymentBuilder(
        build_dir_base=build_dir_base,
        script_file=handler_path,
        requirements_file=requirements_file,
        deployment_zip=deployment_zip,
        deployment_bucket=deployment_bucket,
        deployment_key=deployment_key,
        rkstr8_wheel_path=rkstr8_path
    )

    yield builder

    # builder.tear_down()


class DeploymentBuilder(object):

    def __init__(self, build_dir_base, script_file, requirements_file, deployment_zip, deployment_bucket, deployment_key, rkstr8_wheel_path):
        self.build_dir_base = build_dir_base
        self.tmp_build_dir = None # set in create_deployment_package, used later to tear_down temp build dir
        self.script_file = script_file
        self.requirements_file = requirements_file
        self.rkstr8_wheel_path = rkstr8_wheel_path
        self.deployment_zip = deployment_zip
        self.deployment_bucket = deployment_bucket
        self.deployment_key = deployment_key

    def upload_deployment_package(self):

        S3Upload.upload_file(
            local_path=(Path(self.build_dir_base) / self.deployment_zip).with_suffix('.zip'),
            bucket_name=self.deployment_bucket,
            key_name=self.deployment_key
        )

        deployment_upload_exists = S3Upload.object_exists(
            bucket_name=self.deployment_bucket,
            key_name=self.deployment_key
        )

        # Debug, just delete this line
        return self.deployment_bucket, self.deployment_key

        if not deployment_upload_exists:
            raise RuntimeError('Lambda deployment does not exist in S3. Upload failed?')
        else:
            return self.deployment_bucket, self.deployment_key

    def create_deployment_package(self):
        '''
        1. Create build directory
        2. Copy lambda handler to the build dir
        3. Install rkstr8 wheel to build dir with pip
        4. Zip the contents of build dir for AWS deployment
        '''
        self.tmp_build_dir = tempfile.mkdtemp(prefix='.rkstr8_lambda_bld_', dir=self.build_dir_base)
        try:
            shutil.copyfile(src=self.script_file, dst=os.path.join(self.tmp_build_dir, os.path.basename(self.script_file)))
            subprocess.check_call([sys.executable, "-m", "pip", "install", self.rkstr8_wheel_path, "-t", self.tmp_build_dir])
            prevdir = os.getcwd() # needed for make_archive strangeness
            try:
                os.chdir(Path(self.tmp_build_dir).parent)
                shutil.make_archive(base_name=self.deployment_zip, format='zip', root_dir=self.tmp_build_dir, base_dir='./')
            finally:
                if prevdir:
                    os.chdir(prevdir)
        except BaseException as be:
            try:
                self.tear_down_build_dir()
            except:
                print('Deleting build dir failed.')
            raise be

    def tear_down(self):
        shutil.rmtree(self.tmp_build_dir)
        os.remove((Path(self.build_dir_base) / self.deployment_zip).with_suffix('.zip'))
