import uuid
import yaml
import os
from pathlib import Path
from typing import Dict


class Config:
    TMP_BUILDDIR_PREFIX = '.rkstr8_pkg_data_'
    RKSTR8_DATA_PKG_NAME = 'rkstr8.data'
    DEMO_PIPELINE_SPEC_DIR = 'demo'

    def __init__(self, pipeline_config_file: str, pipeline_spec_file: str, pipeline_spec_dir: str, pkg_data_dir: str, user_args: Dict[str,str]):
        self.pipeline_config_file = pipeline_config_file
        self.pipeline_spec_file = pipeline_spec_file
        self.pipeline_spec_dir = pipeline_spec_dir
        self.pkg_data_dir = pkg_data_dir
        self.user_args = user_args


    def merge_sources(self):
        conf = self.merge_config_sources()
        args = self.user_args
        self.launch_config = LaunchConfig(conf, args)
        self.pipeline_config = PipelineConfig(conf, args)
        self.batch_config = BatchConfig(conf, args)
        self.lambda_config = LambdaConfig(conf, args)
        self.step_functions_config = StepFunctionsConfig(conf, args)
        self.cloudformation_config = CloudFormationConfig(conf, args)
        self.metadata_config = MetaDataConfig(conf, args)
        self.dynamo_db_config = DynamoDbConfig(conf, args)
        return self

    def merge_config_sources(self):
        '''
        Merge and otherwise transform user-provided and application-specific configuration items.
        The string constants declared here should generally not be messed with.
        '''
        pipeline_config_file = self.pipeline_config_file
        pipeline_spec_file = self.pipeline_spec_file
        pipeline_spec_dir = self.pipeline_spec_dir
        pkg_data_dir = self.pkg_data_dir

        conf = {}

        conf['DRY_RUN'] = self.user_args.get('dry_run', False)
        conf['DEMO'] = self.user_args.get('demo', False)

        conf['PIPELINE_SPEC_DIR'] = pipeline_spec_dir
        conf['PKG_DATA_DIR'] = pkg_data_dir

        with open(pipeline_config_file, 'r') as f:
            pipeline_conf = yaml.safe_load(f)

        initialization = pipeline_conf.get('INITIALIZATION')

        conf['TMP_BUILDDIR_BASE'] = pipeline_conf['INITIALIZATION'].get('TMP_BUILDDIR_BASE', pipeline_spec_dir) if initialization else pipeline_spec_dir
        assert (Path(conf['TMP_BUILDDIR_BASE']).is_dir() and Path(conf['TMP_BUILDDIR_BASE']).exists())
        conf['PKG_RKSTR8'] = pipeline_conf['INITIALIZATION'].get('PKG_RKSTR8', False) if initialization else False

        try:
            docker_init = pipeline_conf['INITIALIZATION']['DOCKER']
        except KeyError as e:
            docker_init = None

        conf['DOCKER_BUILD'] = pipeline_conf['INITIALIZATION']['DOCKER']['DOCKER_BUILD'] if docker_init else False
        conf['DOCKER_PUSH'] = pipeline_conf['INITIALIZATION']['DOCKER']['DOCKER_PUSH'] if docker_init else False
        conf['DOCKER_TAGS_MAP'] = pipeline_conf['INITIALIZATION']['DOCKER']['TAGS'] if (conf['DOCKER_BUILD'] or conf['DOCKER_PUSH']) else {}

        conf['DEMO_PIPELINE_SPEC_DIR'] = Config.DEMO_PIPELINE_SPEC_DIR
        conf['PIPELINE_SPEC'] = pipeline_spec_file
        conf['PIPELINE_SPEC_DIR'] = pipeline_spec_dir
        conf['STACK_UID'] = str(uuid.uuid4())[:4]

        conf['RKSTR8_WHEEL_LOCAL_PATH'] = 'dist/rkstr8-0.0.1-py3-none-any.whl'

        try:
            conf['PIPELINE_TASKDEF'] = next(Path(pipeline_spec_dir).glob('*.taskdefs.py'))
            conf['PIPELINE_TASKDEF_REQUIREMENTS'] = next(Path(pipeline_spec_dir).glob('*.taskdefs.requirements.txt'))
        except StopIteration:
            raise ValueError('Could not locate expected taskdef or requirements file in pipeline definition dir')

        # This configuration tree from the pipeline config is passed to the docker container, for, e.g. access to
        # to pre-existing S3 files (as opposed to those that get uploaded)
        conf['TASK_CONFIG'] = pipeline_conf.get('TASK_CONFIG',{})
        conf['LOCAL_ASSETS_DIR'] = pipeline_conf.get('LOCAL_ASSETS_DIR', '')

        conf['POLLER_WAIT_TIME'] = 30
        conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'] = pipeline_conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
        conf['RESOURCE_CFN_TMPL_DEPLOY_KEY'] = conf['STACK_UID']

        # Default params which can/should be overriden by user's pipeline config
        conf['STACK_NAME'] = 'rkstr8-pipeline-default'
        # ECS-optimized container ami, however it's deprecating soon
        # See: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-optimized_AMI.html
        # conf['CLUSTER_BASE_IMAGE'] = 'ami-a58760b3'

        # Update config values for user changeable configuration if the param is in run.yaml
        # If we just did a dict.update the user could clobber a platform key-value pair unwittingly, so we specify which
        # keys are allowed to be overriden in the following array
        overridable_params = ['STACK_NAME', 'RESOURCE_CFN_TMPL_DEPLOY_BUCKET', 'CLUSTER_BASE_IMAGE']
        for param in overridable_params:
            if param in pipeline_conf:
                conf[param] = pipeline_conf[param]

        conf['RKSTR8_PKG_LOCAL_PATH'] = 'rkstr8'
        conf['RKSTR8_PKG_ARCHIVE_LOCAL_PATH_PREFIX'] = 'rkstr8_{}.pkg'.format(conf['STACK_UID'])
        conf['RKSTR8_PKG_ARCHIVE_LOCAL_PATH'] = 'rkstr8_{}.pkg.zip'.format(conf['STACK_UID'])
        conf['RKSTR8_PKG_REMOTE_BUCKET'] = pipeline_conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
        conf['RKSTR8_PKG_REMOTE_KEY'] = conf['RKSTR8_PKG_ARCHIVE_LOCAL_PATH']

        conf['LAMBDA_HANDLER_DIR'] = 'lambda'
        conf['LAMBDA_BASE_TEMPLATE'] = 'cloudformation/fragments/lambda_base.yaml'
        conf['LAMBDA_RENDERED_TEMPLATE'] = 'cloudformation/lambda_resources.stack.yaml'

        conf['STACK_NAME'] = '{}-{}'.format(pipeline_conf['STACK_NAME'], conf['STACK_UID'])
        conf['CFN_FSA_LOGICAL_RESOURCE_ID'] = 'PipelineStateMachine'

        conf['FSA_STATS_PATH'] = '$.Runtime.queue_status'
        conf['SUBMITTER_LOGICAL_ID'] = 'JobSubmitter'
        conf['POLLER_LOGICAL_ID'] = 'JobPoller'
        conf['POLLER_WAIT'] = 20
        conf['FSA_NAME'] = 'SimplePipeline'

        conf['PARENT_TEMPLATE_PATH'] = 'cloudformation/platform_parent.stack.yaml'
        conf['LAMBDA_TEMPLATE_PATH'] = 'rkstr8/infrastructure/lambda_resources.stack.yaml'
        conf['NETWORK_TEMPLATE_PATH'] = 'cloudformation/network_resources.stack.yaml'
        conf['BATCH_TEMPLATE_PATH'] = 'infrastructure/batch_resources.stack.yaml'
        conf['DYNAMO_DB_TEMPLATE_PATH'] = 'cloudformation/dynamo_db_resources.stack.yaml'
        conf['STEPFUNCTIONS_TEMPLATE_PATH'] = 'cloudformation/step_functions_resources.stack.yaml'
        conf['STATE_MACHINE_RESOURCE_FRAGMENT'] = 'cloudformation/fragments/statemachine.json'
        conf['FRAGMENTS_DIR_PATH'] = 'infrastructure/fragments'
        conf['STEPFUNCTIONS_TEMPLATE_PATH_FINAL'] = 'cloudformation/step_functions_resources.stack.rendered.yaml'
        conf['CLUSTER_TEMPL_PATH'] = 'cloudformation/fragments/batch_comp_env_template.yaml'
        conf['JOB_DEF_TEMPL_PATH'] = 'cloudformation/fragments/batch_job_def_template.yaml'
        conf['QUEUE_TEMPL_PATH'] = 'cloudformation/fragments/batch_queue_template.yaml'
        conf['BATCH_TEMPL_PATH'] = 'cloudformation/batch_resources.stack.yaml'
        conf['BATCH_RENDERED_TEMPL_PATH'] = 'cloudformation/batch_resources.stack.rendered.yaml'
        conf['LAMBDA_CFN_PARAM_TEMPLATE_URL'] = 'LambdaTemplateURL'
        conf['NETWORK_CFN_PARAM_TEMPLATE_URL'] = 'NetworkTemplateURL'
        conf['BATCH_CFN_PARAM_TEMPLATE_URL'] = 'BatchTemplateURL'
        conf['STEP_FUNCTIONS_PARAM_TEMPLATE_URL'] = 'StepFunctionsTemplateURL'

        conf['TEMPLATE_LABEL_PATH_MAP'] = {
            'launch': conf['PARENT_TEMPLATE_PATH'],
            'lambda': conf['LAMBDA_TEMPLATE_PATH'],
            'network': conf['NETWORK_TEMPLATE_PATH'],
            'batch': conf['BATCH_TEMPLATE_PATH'],
            'sfn': conf['STEPFUNCTIONS_TEMPLATE_PATH']
        }

        conf['LAMBDA_CFN_ARGMT_TEMPLATE_URL'] = 'https://s3.amazonaws.com/{}/lambda_resources.stack.yaml'.format(conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'])
        conf['NETWORK_CFN_ARGMT_TEMPLATE_URL'] = 'https://s3.amazonaws.com/{}/network_resources.stack.yaml'.format(conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'])
        conf['BATCH_CFN_ARGMT_TEMPLATE_URL'] = 'https://s3.amazonaws.com/{}/batch_resources.stack.yaml'.format(conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'])
        conf['STEP_FUNCTIONS_ARGMT_TEMPLATE_URL'] = 'https://s3.amazonaws.com/{}/step_functions_resources.stack.yaml'.format(conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'])

        conf['JOB_ROLE_CFN_LRI'] = 'GeneralPurposeContainerRole'
        conf['BATCH_EVENT_ENV_VAR_NAME'] = 'PSYCHCORE_PIPELINE_EVENT'
        conf['DDB_TABLE_NAME'] = 'PipelineTable-{}'.format(conf['STACK_UID'])
        conf['STACK_LAUNCH_TIMEOUT_MINUTES'] = 10

        return conf


class BaseConfig(object):
    ''' Primary config interface to application. Application never accesses config dictionary, only properties herein'''

    def __init__(self, config, args):
        self._config = config
        self.args = args

    def get_config_item(self, property_name):
        if property_name not in self._config.keys(): # we don't want KeyError
            return None  # just return None if not found
        return self._config[property_name]

    # TODO deprecrating
    def get_run_config_item(self, property_name):
        task_config = self.get_config_item('TASK_CONFIG')
        return task_config[property_name]

    def get_user_arg(self, property_name):
        return self.args.get(property_name)


class LaunchConfig(BaseConfig):

    @property
    def pkg_data_dir(self):
        return self.get_config_item('PKG_DATA_DIR')

    @property
    def tmp_build_dir_prefix(self):
        return Config.TMP_BUILDDIR_PREFIX

    @property
    def rkstr8_data_pkg_name(self):
        return Config.RKSTR8_DATA_PKG_NAME

    @property
    def do_pkg_rkstr8(self):
        return self.get_config_item('PKG_RKSTR8')

    @property
    def dry_run(self):
        return self.get_config_item('DRY_RUN')

    @property
    def demo(self):
        return self.get_config_item('DEMO')


class BatchConfig(BaseConfig):

    @property
    def job_template_file(self):
        return self.get_config_item('JOB_DEF_TEMPL_PATH')

    @property
    def cluster_template_file(self):
        return self.get_config_item('CLUSTER_TEMPL_PATH')

    @property
    def queue_template_file(self):
        return self.get_config_item('QUEUE_TEMPL_PATH')

    @property
    def base_template_file(self):
        return self.get_config_item('BATCH_TEMPL_PATH')

    @property
    def rendered_template_file(self):
        return self.get_config_item('BATCH_RENDERED_TEMPL_PATH')

    @property
    def job_role_lri(self):
        return self.get_config_item('JOB_ROLE_CFN_LRI')

    @property
    def cluster_ami(self):
        return self.get_config_item('CLUSTER_BASE_IMAGE')

    @property
    def event_env_var_name(self):
        return self.get_config_item('BATCH_EVENT_ENV_VAR_NAME')

    @property
    def ssh_key(self):
        try:
            return self.get_user_arg('key_pair')
        except KeyError:
            return self.get_run_config_item('AWS')['SSH_KEYPAIR']


class MetaDataConfig(BaseConfig):

    @property
    def s3_base_uri(self):
        try:
            return 's3://' + self.get_user_arg('bucket') + '/'
        except KeyError:
            return self.get_run_config_item('PIPELINE')['S3_BASE']

    @property
    def s3_remote_files(self):
        # may or may not be present
        # if not present?
        try:
            return self.get_run_config_item('PIPELINE')['S3_EXTERNAL_FILES']
        except KeyError:
            return []

    @property
    def command_spec_name(self):
        return self.get_run_config_item('PIPELINE')['CMD_TEMPLATES_FILE']

    @property
    def local_assets_dir(self):
        return self.get_config_item('LOCAL_ASSETS_DIR')

    @property
    def init_event_filename(self):
        return 'event.rendered.yaml'


class DynamoDbConfig(BaseConfig):

    @property
    def table_name(self):
        return self.get_config_item('DDB_TABLE_NAME')


class PipelineConfig(BaseConfig):

    @property
    def pipeline_file(self):
        return self.get_config_item('PIPELINE_SPEC')

    @property
    def pipeline_dir(self):
        return self.get_config_item('PIPELINE_SPEC_DIR')

    @property
    def taskdef_path(self):
        return self.get_config_item('PIPELINE_TASKDEF')

    @property
    def taskdef_requirements_path(self):
        return self.get_config_item('PIPELINE_TASKDEF_REQUIREMENTS')

    @property
    def rkstr8_wheel_path(self):
        return self.get_config_item('RKSTR8_WHEEL_LOCAL_PATH')

    # TODO should be a launch config item
    @property
    def repo_root(self):
        return self.get_user_arg('repo_root')

    @property
    def demo_spec_dir(self):
        return self.get_config_item('DEMO_PIPELINE_SPEC_DIR')

    # TODO move to a DockerConfig class and pass into platform init
    @property
    def docker_build(self):
        return self.get_config_item('DOCKER_BUILD')

    @property
    def docker_push(self):
        return self.get_config_item('DOCKER_PUSH')

    @property
    def docker_tags(self):
        return self.get_config_item('DOCKER_TAGS_MAP')


class StepFunctionsConfig(BaseConfig):

    @property
    def state_machine_fragment(self):
        return self.get_config_item('STATE_MACHINE_RESOURCE_FRAGMENT')

    @property
    def step_functions_template(self):
        return self.get_config_item('STEPFUNCTIONS_TEMPLATE_PATH')

    @property
    def step_functions_template_final(self):
        return self.get_config_item('STEPFUNCTIONS_TEMPLATE_PATH_FINAL')

    @property
    def state_machine_resource_id(self):
        return self.get_config_item('CFN_FSA_LOGICAL_RESOURCE_ID')

    @property
    def state_machine_stats_path(self):
        return self.get_config_item('FSA_STATS_PATH')

    @property
    def poller_wait_time(self):
        return self.get_config_item('POLLER_WAIT_TIME')

    @property
    def machine_name(self):
        return self.get_config_item('FSA_NAME')


class CloudFormationConfig(BaseConfig):

    @property
    def job_submitter_id(self):
        return self.get_config_item('SUBMITTER_LOGICAL_ID')

    @property
    def job_poller_id(self):
        return self.get_config_item('POLLER_LOGICAL_ID')

    @property
    def parent_template_path(self):
        return self.get_config_item('PARENT_TEMPLATE_PATH')

    @property
    def network_template_path(self):
        return self.get_config_item('NETWORK_TEMPLATE_PATH')

    @property
    def lambda_rendered_template_path(self):
        return self.get_config_item('LAMBDA_RENDERED_TEMPLATE')

    @property
    def batch_rendered_template_path(self):
        return self.get_config_item('BATCH_RENDERED_TEMPL_PATH')

    @property
    def step_functions_rendered_template_path(self):
        return self.get_config_item('STEPFUNCTIONS_TEMPLATE_PATH_FINAL')

    @property
    def dynamo_db_template_path(self):
        return self.get_config_item('DYNAMO_DB_TEMPLATE_PATH')

    @property
    def asset_bucket(self):
        return self.get_config_item('RESOURCE_CFN_TMPL_DEPLOY_BUCKET')

    @property
    def subnet_az(self):
        try:
            return self.get_user_arg('availability_zone')
        except KeyError:
            return self.get_run_config_item('AWS')['AZ']

    @property
    def network_param_name(self):
        return 'NetworkTemplateURL'

    @property
    def lambda_param_name(self):
        return 'LambdaTemplateURL'

    @property
    def step_functions_param_name(self):
        return 'StepFunctionsTemplateURL'

    @property
    def dynamo_db_param_name(self):
        return 'DynamoDbTemplateURL'

    @property
    def batch_param_name(self):
        return 'BatchTemplateURL'

    @property
    def batch_stack_logical_id(self):
        return 'BatchResourcesStack'

    @property
    def step_functions_stack_logical_id(self):
        return 'StepFunctionResourcesStack'

    @property
    def az_param_name(self):
        return 'GPCESubnetAZ1'

    @property
    def stack_uid_param_name(self):
        return 'StackUID'

    @property
    def stack_name_param_name(self):
        return 'StackName'

    @property
    def stack_name(self):
        return self.get_config_item('STACK_NAME')

    @property
    def stack_uid(self):
        return self.get_config_item('STACK_UID')

    @property
    def create_timeout(self):
        return self.get_config_item('STACK_LAUNCH_TIMEOUT_MINUTES')


class LambdaConfig(BaseConfig):

    @property
    def handler_dir(self):
        return self.get_config_item('LAMBDA_HANDLER_DIR')

    @property
    def build_dir_base(self):
        return self.get_config_item('TMP_BUILDDIR_BASE')

    @property
    def rkstr8_path(self):
        return self.get_config_item('RKSTR8_PKG_LOCAL_PATH')

    @property
    def asset_bucket(self):
        return self.get_config_item('RESOURCE_CFN_TMPL_DEPLOY_BUCKET')

    @property
    def template_base(self):
        return self.get_config_item('LAMBDA_BASE_TEMPLATE')

    @property
    def rendered_template_path(self):
        return self.get_config_item('LAMBDA_RENDERED_TEMPLATE')

    @property
    def time_out(self):
        return str(60*15)
