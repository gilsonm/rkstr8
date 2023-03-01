from rkstr8.conf import Config, PipelineConfig
from rkstr8.cloud.stepfunctions import AsyncPoller, SingleTaskLoop, Task, Fail, Succeed, \
    StateMachine, States, State, flatten, wrap_cfn_variable
from rkstr8.cloud.cloudformation import TemplateProcessor
from rkstr8.cloud import BotoClientFactory, Service
from rkstr8.cloud.dynamodb import DynamoDb
from rkstr8.common.best_effort import DynamoDbFacade, AggregateNodeColoring, StageNodeColoring
from rkstr8.common.container import S3UriManager, S3Transport
import boto3
from boto.s3.key import Key
from dataclasses import dataclass
from botocore.exceptions import WaiterError
from arnparse import arnparse
from pprint import pprint
from urllib.parse import urlparse, urlunparse
from collections import defaultdict
from abc import ABC, abstractmethod
import networkx as nx
import subprocess
from pathlib import Path
import requests
from smart_open import smart_open
import docker
import tempfile
import functools
import importlib.resources as importlib_resources
import os
import shutil
import yaml
import json
import boto
import re


class AssetStager:

    def __init__(self, config: Config):
        self.metadata_config = config.metadata_config

    def stage(self):
        s3_resource = boto3.resource('s3')
        # get s3 boto3 resource
        # s3_resource = BotoClientFactory().get_client(Service.S3)
        if not self.metadata_config.local_assets_dir:
            return

        uri_manager = S3UriManager(self.metadata_config.s3_base_uri)
        transport = S3Transport(s3_resource)

        local_assets_dir = Path(self.metadata_config.local_assets_dir)

        for asset in local_assets_dir.iterdir():
            if asset.is_file():
                asset_path_str = asset.as_posix()
                asset_uri = uri_manager.uri_for_asset(asset.name)
                transport.upload(asset_path_str, asset_uri)


class RenderedTemplateCleaner:

    def __init__(self, config: Config):
        self.lambda_config = config.lambda_config
        self.batch_config = config.batch_config
        self.step_functions_config = config.step_functions_config

    def clean(self, resolver):
        templates = map(resolver,
            [
            self.lambda_config.rendered_template_path,
            self.batch_config.rendered_template_file,
            self.step_functions_config.step_functions_template_final
            ]
        )
        paths_to_remove = [Path(generated_file) for generated_file in templates]

        for path in paths_to_remove:
            path.unlink()


class CloudFormationStackWaiter:

    def __init__(self, config: Config):
        self.config = config.cloudformation_config

    def wait(self):
        client = boto3.client('cloudformation')
        waiter = client.get_waiter('stack_create_complete')
        stack_name = self.config.stack_name

        try:
            waiter.wait(
                StackName=stack_name,
                WaiterConfig={
                    'Delay': 30,
                    'MaxAttempts': 120
                }
            )
        except WaiterError:
            print('Error waiting for stack, {}, to create. Fatal.'.format(stack_name))
            raise


class StepFunctionsEventUploader():

    def __init__(self, config: Config):
        self.metadata_config = config.metadata_config

    def upload(self, event):

        uri_manager = S3UriManager(self.metadata_config.s3_base_uri)
        event_s3_uri = uri_manager.uri_for_event('base')

        with smart_open(event_s3_uri, 'w') as event_stream:
            event_stream.write(
                yaml.safe_dump(event)
            )

        return event_s3_uri, self.metadata_config.init_event_filename


class PopulateInitialColoring:

    def __init__(self, config: Config):
        self.table_name = config.dynamo_db_config.table_name

    def populate(self, stage_graph, aggregate_graph):

        dynamo_db = DynamoDbFacade(
            dynamo_db=DynamoDb(
                table_name=self.table_name,
                dynamo_db_resource=boto3.resource('dynamodb')
            )
        )

        # Initial coloring:
        #
        # Aggregates:
        #   - RecordId
        #   - Status: WAITING
        #
        # Stages:
        #   - RecordId
        #   x QueueId
        #   x JobId

        for aggregate in aggregate_graph:
            dynamo_db.put_agg_coloring(
                agg_name=aggregate.name,
                coloring=AggregateNodeColoring(status='WAITING')
            )

        for stage in stage_graph:
            dynamo_db.put_stage_coloring(
                stage_name=stage.name,
                coloring=StageNodeColoring(job_id=None, queue_id=None)
            )


class StepFunctionsExecutor:

    def __init__(self, config: Config):
        self.cloudformation_config = config.cloudformation_config
        self.step_functions_config = config.step_functions_config

        self.cloudformation = boto3.resource('cloudformation')
        self.sfn_client = boto3.client('stepfunctions')

    def _get_stepfunctions_stack(self):
        parent_stack_name = self.cloudformation_config.stack_name
        step_functions_stack_name = self.cloudformation_config.step_functions_stack_logical_id

        step_functions_stack_resource = self.cloudformation.StackResource(parent_stack_name, step_functions_stack_name)
        step_functions_stack_resource_arn = arnparse(step_functions_stack_resource.physical_resource_id)
        step_functions_stack = self.cloudformation.Stack(step_functions_stack_resource_arn.resource.split('/')[0])

        return step_functions_stack

    def start_execution(self, event_s3_uri):
        step_functions_stack = self._get_stepfunctions_stack()

        machine_arn = step_functions_stack.Resource(self.step_functions_config.state_machine_resource_id).physical_resource_id
        execution_name = '-'.join(('PipelineExecution', self.cloudformation_config.stack_uid))
        event_str = json.dumps(
            {'event_s3_uri': event_s3_uri}
        )

        response = self.sfn_client.start_execution(
            stateMachineArn=machine_arn,
            name=execution_name,
            input=event_str
        )

        try:
            machine_execution_arn = response['executionArn']
        except KeyError:
            print('Failed to retreive executionArn from start_execution response')
        else:
            print('execution_arn: {}'.format(machine_execution_arn))


class StepFunctionsEventBuilder:

    def __init__(self, config: Config):
        self.pipeline_config = config.pipeline_config
        self.cloudformation_config = config.cloudformation_config
        self.metadata_config = config.metadata_config
        self.dynamo_db_config = config.dynamo_db_config
        self.batch_config = config.batch_config

        self.cloudformation = boto3.resource('cloudformation')

    def _build_runtime(self):
        # initially empty; populated at runtime by Lambda scheduler
        return {
        }

    def __camel_to_snake_case(self, name):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    def __job_def_name_to_stage_name(self, job_def_name):
        parts = job_def_name.split('JobDef')
        stage_name_camel_case = parts[0]
        stage_name = self.__camel_to_snake_case(stage_name_camel_case)
        return stage_name

    def _build_batch(self):

        parent_stack_name = self.cloudformation_config.stack_name
        batch_stack_name = self.cloudformation_config.batch_stack_logical_id

        batch_stack_resource = self.cloudformation.StackResource(parent_stack_name, batch_stack_name)
        batch_stack_resource_arn = arnparse(batch_stack_resource.physical_resource_id)
        print('Batch stack arn: {}'.format(batch_stack_resource_arn))
        batch_stack = self.cloudformation.Stack(batch_stack_resource_arn.resource.split('/')[0])
        print('Batch stack: {}'.format(batch_stack))

        batch = {
            'JobDefinitions': {},
            'JobQueues': {},
            'EventEnvVarName': self.batch_config.event_env_var_name
        }

        for resource_summary in batch_stack.resource_summaries.all():
            print('Resource summary: {}'.format(resource_summary))

            if resource_summary.resource_type == 'AWS::Batch::JobDefinition':

                resource_arn = arnparse(resource_summary.physical_resource_id)

                # 1. convert job def name to stage name (yikes)
                job_def_name = resource_arn.resource

                stage_name_for_job = self.__job_def_name_to_stage_name(job_def_name)

                # 2. get job def id from arnparse object
                job_def_id = resource_arn.resource

                print(job_def_name, stage_name_for_job, job_def_id)

                # 3. add to returnable structure
                batch['JobDefinitions'][stage_name_for_job] = job_def_id
            elif resource_summary.resource_type == 'AWS::Batch::JobQueue':

                resource_arn = arnparse(resource_summary.physical_resource_id)

                # 1. convert resource to queue_name
                queue_name = resource_arn.resource.split('-')[0]
                batch['JobQueues'][queue_name ] = resource_arn.resource

        return batch

    def _build_pipeline(self):
        pipeline_spec_path = Path(self.pipeline_config.pipeline_file)

        with pipeline_spec_path.open() as f:
            spec = yaml.safe_load(f)

        return spec['Pipeline']

    def _build_metadata(self):
        return {
            's3_base': self.metadata_config.s3_base_uri,
            'cmd_tmpl_file': self.metadata_config.command_spec_name,
            's3_remote_files': self.metadata_config.s3_remote_files
        }

    def _build_dynamo_db(self):
        return {
            'table_name': self.dynamo_db_config.table_name
        }

    def build_event(self):

        event = {
            'Runtime': self._build_runtime(),
            'Batch': self._build_batch(),
            'DynamoDb': self._build_dynamo_db(),
            'MetaData': self._build_metadata(),
            'Pipeline': self._build_pipeline()
        }

        return event, self.metadata_config.init_event_filename


class CloudFormationTemplateDeployer:

    def __init__(self, config: Config):
        self.cloud_formation_config = config.cloudformation_config
        self.s3_connection = None
        self.s3_bucket = None

    def _get_connection(self):
        if not self.s3_connection:
            self.s3_connection = boto.connect_s3()
        return self.s3_connection

    def _load_bucket(self, bucket_name):
        if not self.s3_bucket:
            conn = self._get_connection()
            self.s3_bucket = conn.get_bucket(bucket_name)
        return self.s3_bucket

    def _s3_url_for_key(self, bucket_name, key_name):
        return urlunparse(('s3', bucket_name, key_name, None, None, None))

    def _https_url_for_key(self, key):
        return key.generate_url(expires_in=0)

    # TODO: Make a context manager that validates existence on clean-up
    def upload(self, template_file, bucket_name, key_name):
        s3_bucket = self._load_bucket(bucket_name)
        s3_key = Key(s3_bucket)
        s3_key.key = key_name
        s3_key.set_contents_from_filename(template_file)

        # return self._s3_url_for_key(bucket_name, key_name)
        return self._https_url_for_key(s3_key)

    def deploy(self, pkg_resource_resolver):

        templates_to_keys = {
            self.cloud_formation_config.parent_template_path: 'platform.stack.yaml',
            self.cloud_formation_config.network_template_path: 'network.stack.yaml',
            self.cloud_formation_config.lambda_rendered_template_path: 'lambda.stack.yaml',
            self.cloud_formation_config.batch_rendered_template_path: 'batch.stack.yaml',
            self.cloud_formation_config.step_functions_rendered_template_path: 'stepfunctions.stack.yaml',
            self.cloud_formation_config.dynamo_db_template_path: 'dynamo_db.stack.yaml'
        }

        templates_to_params = {
            self.cloud_formation_config.network_template_path: self.cloud_formation_config.network_param_name,
            self.cloud_formation_config.lambda_rendered_template_path: self.cloud_formation_config.lambda_param_name,
            self.cloud_formation_config.batch_rendered_template_path: self.cloud_formation_config.batch_param_name,
            self.cloud_formation_config.step_functions_rendered_template_path: self.cloud_formation_config.step_functions_param_name,
            self.cloud_formation_config.dynamo_db_template_path: self.cloud_formation_config.dynamo_db_param_name
        }

        params_to_s3_urls = dict()
        platform_s3_url = None

        for template_file, key_name in templates_to_keys.items():

            s3_url = self.upload(
                template_file=pkg_resource_resolver(template_file),
                bucket_name=self.cloud_formation_config.asset_bucket,
                key_name=key_name
            )

            try:
                param = templates_to_params[template_file]
            except KeyError:
                if 'platform.stack.yaml' == key_name:
                    platform_s3_url = s3_url
                else:
                    raise
            else:
                params_to_s3_urls[param] = s3_url

        return platform_s3_url, params_to_s3_urls


class CloudFormationStackLauncher:

    def __init__(self, config: Config):
        self.cloud_formation_config = config.cloudformation_config

    def params(self, params_to_template_s3_urls):
        params = [
            {
                'ParameterKey': self.cloud_formation_config.az_param_name,
                'ParameterValue': self.cloud_formation_config.subnet_az
            },
            {
                'ParameterKey': self.cloud_formation_config.stack_uid_param_name,
                'ParameterValue': self.cloud_formation_config.stack_uid
            },
            {
                'ParameterKey': self.cloud_formation_config.stack_name_param_name,
                'ParameterValue': self.cloud_formation_config.stack_name
            }
        ]

        for param_key, param_value in params_to_template_s3_urls.items():
            params.append(
                {
                    'ParameterKey': param_key,
                    'ParameterValue': param_value
                }
            )

        return params

    def launch(self, platform_template_s3_url, params_to_template_s3_urls):
        cfn_client = BotoClientFactory.client_for(Service.CLOUDFORMATION)

        response = cfn_client.create_stack(
            StackName=self.cloud_formation_config.stack_name,
            TemplateURL=platform_template_s3_url,
            Parameters=self.params(params_to_template_s3_urls),
            DisableRollback=False,
            TimeoutInMinutes=self.cloud_formation_config.create_timeout,
            Capabilities=['CAPABILITY_IAM'],
            Tags=[
                {
                    'Key': 'Name',
                    'Value': self.cloud_formation_config.stack_name
                }
            ]
        )

        pprint(response)


class StateMachineRenderer:

    def __init__(self, config: Config):
        self.step_functions_config = config.step_functions_config
        self.cloudformation_config = config.cloudformation_config

    def wrap_import_variable(self, output_variable_suffix):
        return {
            'Fn::ImportValue': {
                'Fn::Sub': '-'.join(('${StackUID}', output_variable_suffix))
            }
        }

    def render(self, machine, resolver):

        build = machine.build()

        state_machine_json = json.dumps(
            build,
            indent=2,
            sort_keys=False
        )

        submitter = self.cloudformation_config.job_submitter_id
        # poller = self.cloudformation_config.job_poller_id

        substitutions = {
            submitter: self.wrap_import_variable(submitter),
            # poller: self.wrap_import_variable(poller)
        }

        state_machine_fragment = resolver(self.step_functions_config.state_machine_fragment)
        with open(state_machine_fragment) as fragment:
            state_machine_resource = yaml.safe_load(fragment)

        state_machine_resource['Properties']['DefinitionString']['Fn::Sub'] = [
            state_machine_json,
            substitutions
        ]

        sfn_template_file = resolver(self.step_functions_config.step_functions_template)
        sfn_template_base = [sfn_template_file]

        # Define the template rendering pipeline
        sfn_template_rendered = TemplateProcessor(sfn_template_base) \
            .from_yaml(as_path=True) \
            .add_resource(self.step_functions_config.state_machine_resource_id, state_machine_resource) \
            .to_yaml()

        # Run the template rendering pipeline, leaving results in list
        sfn_template = list(sfn_template_rendered)[0]

        step_functions_template_final = resolver(self.step_functions_config.step_functions_template_final)
        with open(step_functions_template_final, 'w') as sfn_fh:
            sfn_fh.write(sfn_template)


class LinearizedExecutionPlanner:

    GLOBAL_FAIL = Fail('application_fail')

    def __init__(self, state_machine_config, cloudformation_config):
        self.state_machine_config = state_machine_config
        self.cloudformation_config = cloudformation_config
        self.start_state_name = None

    def _is_not_concurrent(self, aggregate):
        return aggregate.type != 'concurrent'

    def _wrap_token_for_substitution(self, token):
        return '${' + token + '}'

    def build_chain(self, stage_graph, aggregate_graph):

        # return both a list of AsyncPollers wired together
        # and a graph of the aggregates in those pollers
        # (which may differ from aggregate_graph)

        chain = []

        stats_path = self.state_machine_config.state_machine_stats_path
        submitter = self._wrap_token_for_substitution(self.cloudformation_config.job_submitter_id)
        poller = self._wrap_token_for_substitution(self.cloudformation_config.job_poller_id)

        aggregate_order = list(nx.topological_sort(aggregate_graph))

        # construct unwired (no edge) graph of AsyncPollers from topo sort
        for idx, aggregate in enumerate(aggregate_order):

            if self._is_not_concurrent(aggregate):

                submitter_name = '_'.join((aggregate.name, 'task'))
                poller_name = '_'.join((aggregate.name, 'poller'))

                if idx == 0:
                    self.start_state_name = submitter_name

                polled_task = AsyncPoller(
                    async_task=Task( # has $.Runtime.CurrStage => knows what to execute
                        name=submitter_name,
                        resource=submitter,
                        next=poller_name
                    ),
                    pollr_task=Task( # has $.Runtime.CurrStage => can get Queue to poll from Object Model
                        name=poller_name,
                        resource=poller,
                        result_path=stats_path
                    ),
                    faild_task=LinearizedExecutionPlanner.GLOBAL_FAIL,
                    succd_task=None, # Must be set after iteration over topological sort. We need name of next AsyncPoller Submitter Task or if none, Succeed.
                    stats_path=stats_path,
                    pollr_wait_time=self.state_machine_config.poller_wait_time
                )

                chain.append(polled_task)

            else:
                raise NotImplementedError('Currently does not support Concurrent Aggregates.')

        return chain, aggregate_order

    def wire_chain(self, chain):

        for idx, async_poller in enumerate(chain):
            next_or_succeed = chain[idx+1].async_task_state.name if idx+1 < len(chain) else 'application_success'
            async_poller.poll_success_state = next_or_succeed

    def graph_from_aggregate_order(self, aggregate_order):

        aggregate_order_graph = nx.DiGraph()

        aggregate_order_graph.add_nodes_from(aggregate_order)

        for idx, aggregate in enumerate(aggregate_order):
            next_aggregate = aggregate_order[idx + 1] if idx + 1 < len(aggregate_order) else None
            if next_aggregate:
                aggregate_order_graph.add_edge(aggregate, next_aggregate)

        return aggregate_order_graph


    @staticmethod
    def flatten(*state_composites):
        '''
        Take in variable number of either stepfunctions_mx.States
        or lists of them, return flattened list
        :param state_composites:
        :return:
        '''
        flattened = []
        for scomp in state_composites:
            if isinstance(scomp, State):
                flattened.append(scomp)
            elif isinstance(scomp, list):
                are_states = [isinstance(item, State) for item in scomp]
                if not all(are_states):
                    raise ValueError('Not every component is a stepfunctions_mx.State')
                flattened.extend(scomp)
        return flattened

    def plan(self, stage_graph, aggregate_graph):
        chain, aggregate_order = self.build_chain(stage_graph, aggregate_graph)
        self.wire_chain(chain)

        aggregate_execution_graph = self.graph_from_aggregate_order(aggregate_order)

        states_nested = [async_poller.states() for async_poller in chain]
        states_nested.append(Succeed('application_success'))

        machine = StateMachine(
            name=self.state_machine_config.machine_name,
            start=self.start_state_name,
            states=States(*self.flatten(*states_nested))
        )

        print(yaml.dump(machine.build()))

        return machine, aggregate_execution_graph


class BestEffortStateMachineBuilder:

    def __init__(self, config: Config):
        self.state_machine_config = config.step_functions_config
        self.cloudformation_config = config.cloudformation_config

    def plan(self):

        best_effort_name = 'BestEffort-Task'
        best_effort_variable = wrap_cfn_variable(self.cloudformation_config.job_submitter_id)
        stats_path = self.state_machine_config.state_machine_stats_path

        loop = SingleTaskLoop(
            async_task=Task(
                name=best_effort_name,
                resource=best_effort_variable,
                result_path=stats_path
            ),
            faild_task=Fail(name='pipeline_fail'),
            succd_task=Succeed(name='pipeline_success'),
            stats_path=stats_path,
            pollr_wait_time=self.state_machine_config.poller_wait_time
        )

        machine = StateMachine(
            name=self.state_machine_config.machine_name,
            start=best_effort_name,
            states=States(*flatten(loop.states()))
        )

        print(yaml.dump(machine.build()))

        return machine


class Rkstr8PackageResourceCopy:

    def __init__(self, prefix, pkg_name, to_dir=os.getcwd()):
        self.tmp_build_dir_prefix = prefix
        self.rkstr8_data_pkg_name = pkg_name
        self.tmp_build_dir_base = to_dir

    def make_temp_dir(self, blocking=True):
        tempdir = tempfile.mkdtemp(prefix=self.tmp_build_dir_prefix, dir=self.tmp_build_dir_base)
        def cleanup_temp_dir(pkg_data_tmpdir):
            import shutil
            shutil.rmtree(pkg_data_tmpdir)
        return tempdir, functools.partial(cleanup_temp_dir, pkg_data_tmpdir=tempdir)

    def rglob_package(self, package, on_file, tempdir):
        for dir_or_file_name in importlib_resources.contents(package):
            if importlib_resources.is_resource(package, dir_or_file_name):
                on_file(package, dir_or_file_name, tempdir)
            else:
                subdir = os.path.join(tempdir, dir_or_file_name)
                os.makedirs(subdir, exist_ok=True)
                self.rglob_package('.'.join([package, dir_or_file_name]), on_file, subdir)

    def copy(self, tempdir):
        rkstr8_data_pkg = self.rkstr8_data_pkg_name

        def on_file(package, resource, dst):
            with importlib_resources.path(package, resource) as p:
                shutil.copy(p, dst)

        self.rglob_package(rkstr8_data_pkg, on_file, tempdir)


class Rkstr8WheelManager:
    """
    Build a wheel for the rkstr8 package from config data using setuptools.
    The wheel is for '$ pip installing' rkstr8 and its deps in the pipeline image, so taskdefs.py can import it.
    Why? You make changes to source tree. Or when you need the wheel in a context which doesn't have connection to PYPI.
    """
    def __init__(self, config: PipelineConfig):
        self.config = config.pipeline_config

    def get_wheel(self, build=False):
        if build:
            return self.build_wheel()
        # TODO If you just want the wheel we'll get it from PYPI using requests
        # For now just return a local hardcopy
        return Path(self.config.rkstr8_wheel_path).absolute()

    def build_wheel(self):
        # We must have cloned the git repo or otherwise have access to the source tree
        # Need config for that
        if os.path.exists('./build.sh'):
            subprocess.check_call(['./build.sh'], cwd=self.config.repo_root)
            return Path(self.config.rkstr8_wheel_path).absolute()
        else:
            raise ValueError('Could not find build.sh. Run from the root of the repo in order to build wheel')

    def get_pypi_wheel(self):

        # Replace 'my_package' and '1.0.0' with the package name and version you want to download
        package_name = 'rkstr8'
        package_version = '0.0.1'

        # Send a GET request to the PyPI API to get the download URL for the wheel file
        url = f'https://pypi.org/pypi/{package_name}/{package_version}/json'
        response = requests.get(url)
        data = response.json()
        urls = data['releases'][package_version][0]['urls']
        wheel_url = next((url['url'] for url in urls if url['packagetype'] == 'bdist_wheel'), None)

        # Download the wheel file
        if wheel_url:
            response = requests.get(wheel_url)
            # TODO how to get path
            with open(f'{package_name}-{package_version}.whl', 'wb') as f:
                f.write(response.content)
        else:
            raise ValueError(f'Error downloading wheel file {package_name} {package_version}')

        # TODO ensure what we've downloaded is in the right place

        return config.rkstr8_wheel_path


@dataclass
class BuildableDockerImage:
    dockerfile: Path
    build_script: Path
    image: docker.models.images.Image
    tag: str

class DockerImageManager:
    """Discovers, builds, and pushes docker images for pipeline tasks"""
    def __init__(self, config: Config):
        self.pipeline_config = config.pipeline_config
        self.buildable_images = dict()

    def discover(self):
        '''Builds a map of dockerfiles and build scripts to buildable_images'''
        # map keyed by parent dir of dockerfile and build script
        buildable_images = self.buildable_images
        config = self.pipeline_config
        exclude = str(config.pipeline_dir / '.rkstr8')
        for path in Path(config.pipeline_dir).rglob('*'):
            if str(path).startswith(exclude): continue
            if path.is_file():
                pathdir = path.parent
                if path.name == 'Dockerfile' or path.name == 'build.sh':
                    buildable_image = buildable_images.setdefault(pathdir,
                        BuildableDockerImage(dockerfile=None,
                                             build_script=None,
                                             image=None,
                                             tag=None)
                    )
                    if pathdir.name in config.docker_tags:
                        buildable_image.tag = config.docker_tags[pathdir.name]
                    if path.name == 'Dockerfile':
                        buildable_image.dockerfile = path
                    if path.name == 'build.sh':
                        buildable_image.build_script = path

        # if there are no dockerfiles and the pipeline config says to build then discovery fails
        if not buildable_images and (config.docker_build):
            raise ValueError('Configured to buil but no dockerfiles found.')

    def _copy(self, path, dst, blocking=False):
        result_path = (dst / path.name).absolute()
        shutil.copy(path, dst)
        if not blocking:
            return result_path
        MAX_WAIT = 10
        waiting = 0
        while not os.path.exists(path):
            if waiting >= MAX_WAIT:
                raise TimeoutError('Timed out waiting for %s to be copied' % path)
            time.sleep(1)
            waiting += 1
        if os.path.isfile(path):
            print('copied %s' % path)
            return result_path
        else:
            raise ValueError("%s isn't a file!" % path)

    def build(self):
        if not self.buildable_images:
            return False
        client = docker.from_env()
        for context, buildable_image in self.buildable_images.items():
            # call a subprocess to run the build script if it exists
            if buildable_image.build_script:
                # if a build.sh script exists then we call it in place of the automated build
                print(buildable_image.build_script)
                # subprocess.check_call(buildable_image.build_script)
                continue

            # attept to build the docker image from the dockerfile and enviornment prepared by the build script
            if buildable_image.dockerfile:
                taskdef = self.pipeline_config.taskdef_path.absolute()
                requirements = self.pipeline_config.taskdef_requirements_path.absolute()
                rkstr8_wheel = Path(self.pipeline_config.rkstr8_wheel_path)

                taskdef_in_context = self._copy(taskdef, context, blocking=True)
                requirements_in_context = self._copy(requirements, context, blocking=True)
                rkstr8_in_context = self._copy(rkstr8_wheel, context, blocking=True)

                build_args = {
                    'RKSTR8_WHEEL': rkstr8_wheel.name,
                    'TASK_DEF_SCRIPT': taskdef.name,
                    'TASK_DEF_REQS': requirements.name
                }

                print(f'Building image from {context.name} ...')

                if buildable_image.tag:
                    tag = buildable_image.tag
                    print(f'Found tag for image in pipeline config. Using {tag}')
                else:
                    tag=f'{context.name.lower()}:latest'
                    print(f'Failed to find tag for image in pipeline config. Using default {tag}')

                image, logs = client.images.build(
                    path=context.absolute().as_posix(),
                    nocache=True,
                    buildargs=build_args,
                    tag=tag
                )

                buildable_image.image = image

                print(f'Built image from {context.name} with ID: {image.id} and tags: {image.tags}')

                taskdef_in_context.unlink()
                requirements_in_context.unlink()
                rkstr8_in_context.unlink()

    def push(self):
        ''' Only push images that were built by the system, those with a build script are not pushed '''
        if not self.buildable_images or (not self.pipeline_config.docker_push):
            return False
        print('pushing docker images')
        client = docker.from_env()
        for buildable_image in self.buildable_images.values():
            if buildable_image.build_script:
                print(f'Skipping push of image {buildable_image.tag} because it was built by a build script')
                continue
            if buildable_image.image:
                print(f'Pushing image {buildable_image.image.tags}')
                img_name, tag = buildable_image.image.tags[0].split(':') # 'hellotask'[0], '1.0'[1]
                resp=client.images.push(
                    repository=img_name,
                    tag=tag,
                    stream=True,
                    decode=True
                )
                for line in resp:
                    print(line)
