import boto3
import hashlib
import os
import yaml
import json
import shutil
import subprocess
from contextlib import contextmanager
from uuid import uuid4
from collections.abc import Sequence
from urllib.parse import urlparse, urlunparse
from pathlib import Path
from smart_open import smart_open
from botocore.exceptions import ClientError
from rkstr8.clients import Event, EventUnmarshaller, UnmarshallingContext
from rkstr8.cloud.dynamodb import DynamoDb
from rkstr8.common.container import S3UriManager, S3Transport
from rkstr8.cloud.s3 import s3_uri_exists

# Bootstrapping issue.  This is already defined in BatchConfig, and used in Submitter.
# However, to pull the event from the environment to get BatchConfig dependency locations requires some "value-0" that
# is known a priori in order to get the event or the BatchConfig dependencies. In either case, "value-0" must be defined
# multiple times, treated as a convention.
PIPELINE_EVENT_ENV_VAR_NAME = 'PSYCHCORE_PIPELINE_EVENT'
BATCH_ARRAY_IDX_VAR_NAME = 'AWS_BATCH_JOB_ARRAY_INDEX'
ARRAY_SIZE_VAR_NAME = 'ARRAY_SIZE'
EVENT_S3_URI_KEY = 'event_s3_uri'


INPUT_MANIFEST_COMMENT_CHAR = '#'

S3_RESOURCE = boto3.resource('s3')

DOCKER_VOLUME_PATH_STR = '/home/localDir/'
SHARE_VOLUME_PATH_STR = '/home/fsx/'

CONTAINER = 'container'
RESULTS = 'results'


class ContainerServices:

    def __init__(self):
        self.event_api = None
        self.unmarshalling_context = UnmarshallingContext()

    def _unmarshall(self):
        if not self.event_api:
            self._set_event_api_from_environment()

        EventUnmarshaller(self.event_api, self.unmarshalling_context).unmarshall_to_context()

    def __initialize_if_needed(self):
        if not self.unmarshalling_context.pipeline:
            self._unmarshall()

    def next_container_uid(self):
        return str(uuid4())

    def get_worker_id(self):
        self.__initialize_if_needed()

        if self.is_array_job(self.get_curr_stage()):
            try:
                array_size = int(os.environ.get(ARRAY_SIZE_VAR_NAME))
            except TypeError:
                raise ValueError('')

            return '0' if array_size == 1 else os.environ.get(BATCH_ARRAY_IDX_VAR_NAME)
        else:
            return None

    def is_array_job(self, stage_name):
        self.__initialize_if_needed()

        curr_stage = self.unmarshalling_context.pipeline.get_stage(stage_name)
        curr_aggr = self.unmarshalling_context.pipeline.get_aggregate_for_stage(curr_stage)

        return curr_aggr.type == 'array'

    def _set_event_api_from_environment(self):

        ref_event_json = os.environ.get(PIPELINE_EVENT_ENV_VAR_NAME)
        if not ref_event_json:
            raise ValueError('{} not defined in shell environment.'.format(PIPELINE_EVENT_ENV_VAR_NAME))

        value_event_s3_uri = json.loads(ref_event_json)[EVENT_S3_URI_KEY]

        # download and unmarshall yaml to Python collection
        with smart_open(value_event_s3_uri) as event_stream:
            event_collection = yaml.safe_load(event_stream)

        self.event_api = Event(event_collection)


    def is_array_result(self, stage_name):
        self.__initialize_if_needed()

        stage = self.unmarshalling_context.pipeline.get_stage(stage_name)
        aggregate = self.unmarshalling_context.pipeline.get_aggregate_for_stage(stage)

        return aggregate.type == 'array'

    def env_for_stage(self, stage_name):
        self.__initialize_if_needed()

        stage = self.unmarshalling_context.pipeline.get_stage(stage_name)
        job = stage.job
        return job.env

    def get_cmd_spec_name(self):
        self.__initialize_if_needed()
        return self.event_api.get_cmd_spec_name()

    def __get_s3_remote_files(self):
        self.__initialize_if_needed()
        return self.event_api.get_s3_remote_files()

    def remote_asset_uri(self, filename):

        print('remote_asset_uri: %s' % filename)

        remote_uris = self.__get_s3_remote_files()
        name_uri_map = {Path(urlparse(uri).path).name: uri for uri in remote_uris}

        print('name_uri_map')
        print(name_uri_map)

        try:
            return name_uri_map[filename]
        except KeyError:
            return None

    def get_curr_stage(self):
        self.__initialize_if_needed()
        return self.event_api.get_curr_stage()

    def get_input_manifest_for_stage(self, stage_name):
        self.__initialize_if_needed()
        pipeline = self.unmarshalling_context.pipeline
        stage = pipeline.get_stage(stage_name)
        aggregate = pipeline.get_aggregate_for_stage(stage)
        input_ = pipeline.get_input(aggregate.name)
        return input_.manifest

    def get_table_name(self):
        self.__initialize_if_needed()
        return self.event_api.get_table_name()

    def get_s3_base_uri(self):
        self.__initialize_if_needed()
        return self.event_api.get_s3_base_uri()


class DynamoDbFacade:

    def __init__(self, dynamo_db):
        self.dynamo_db = dynamo_db

    def put_result(self, stage_name, result_name, worker_id, local_path, result_s3_uri):

        self.dynamo_db.put_record(
            namespace=CONTAINER,
            partition=RESULTS,
            content_parts=[stage_name, result_name, worker_id],
            item={
                'StageName': stage_name,
                'ResultName': result_name,
                'WorkerId': str(worker_id),
                'S3Uri': result_s3_uri,
                'LocalPath': local_path
            }
        )

    def get_result(self, stage_name, result_name, worker_id):

        return self.dynamo_db.get_record(
            namespace=CONTAINER,
            partition=RESULTS,
            content_parts=[stage_name, result_name, worker_id]
        )


class VolumePathManager:

    def __init__(self, container_uid, docker_volume_path):
        self.container_uid = container_uid
        self.docker_volume_path = docker_volume_path

        if not Path(self.docker_volume_path).is_absolute():
            raise ValueError('Docker volume path must be absolute.')

        self.working_dir = self.__working_dir()

    def __working_dir(self):
        volume = Path(self.docker_volume_path)
        uid = Path(self.container_uid)
        return (volume / uid).as_posix()

    def __extend_working_dir(self, path_components):
        working_dir = Path(self.working_dir)

        result = working_dir
        for p in path_components:
            result = result / p

        return result.as_posix()

    @staticmethod
    def path_partition_names():
        return ['external', 'assets', 'metadata', 'results', 'tmp']

    def path_for_tmp(self):
        return self.__extend_working_dir(['tmp'])

    def path_for_external(self, file_name):
        return self.__extend_working_dir(['external', file_name])

    def path_for_asset(self, file_name):
        return self.__extend_working_dir(['assets', file_name])

    def path_for_metadata(self):
        return self.__extend_working_dir(['metadata', 'metadata.yaml'])

    def path_for_result(self, stage_name, result_name, worker_id, filename):
        worker_id_or_empty = worker_id if worker_id else ''
        return self.__extend_working_dir(['results', stage_name, result_name, worker_id_or_empty, filename])


class SharePathManager:

    def __init__(self, share_volume_path):
        self.share_volume_path = share_volume_path

        if not Path(self.share_volume_path).is_absolute():
            raise ValueError('Share path must be absolute.')

    def path_for_root_file(self, filename):

        file_path = Path(self.share_volume_path) / filename

        if file_path.exists():
            raise ValueError('Path exists')
        else:
            return str(file_path)


class DataLayer:

    def __init__(self, dynamo_db, s3_transport, container_services, volume_path_manager, share_path_manager, s3_uri_manager):
        self.dynamo_db = dynamo_db
        self.s3_transport = s3_transport
        self.container_services = container_services
        self.volume_path_manager = volume_path_manager
        self.share_path_manager = share_path_manager
        self.s3_uri_manager = s3_uri_manager

    ## Context and File Management ##

    def make_working_dir_in_volume(self):
        working_dir = self.volume_path_manager.working_dir
        Path(working_dir).mkdir(parents=True, exist_ok=True)

        for partition in self.volume_path_manager.path_partition_names():
            (Path(working_dir) / Path(partition)).mkdir(parents=True, exist_ok=True)

    def remove_working_dir_in_volume(self):
        working_dir = self.volume_path_manager.working_dir
        shutil.rmtree(working_dir)

    def path_for_volume_file(self, fname):
        return self.volume_path_manager.path_for_external(fname)

    def path_for_share_file(self, fname):
        return self.share_path_manager.path_for_root_file(fname)

    def path_for_tmp(self):
        return self.volume_path_manager.path_for_tmp()

    def collocate(self, *args):

        links = {arg: self.path_for_volume_file(Path(arg).name) for arg in args}

        for arg in args:
            Path(links[arg]).symlink_to(Path(arg))

        return tuple([links[arg] for arg in args])

    ## External ##

    def download_s3_object(self, s3_uri):
        print('download_s3_object: %s' % s3_uri)
        file_name = Path(urlparse(s3_uri).path).name
        local_path = self.volume_path_manager.path_for_external(file_name)
        self.s3_transport.download(s3_uri, local_path)
        return local_path

    ## User MetaData ##

    def get_metadata(self):
        '''
        - Use S3UriManager to get metadata URI, metadata_uri
        - Use VolumeManager to get local path, local_path
        - Use S3Transport to download(metadata_uri, local_path)
        - Use PyYaml to safe_load(local_path)
        - Return unmarshalled collection

        NOTE:
            Client can grapple with the schema, since it's not workflow-independent, and inherently user-structured data.
            There is no intention to add writable metadata. Only results (files).
        '''
        raise NotImplementedError()

    ## User Assets ##

    def __get_asset(self, asset_uri, file_name):
        local_asset_path = self.volume_path_manager.path_for_asset(file_name)
        self.s3_transport.download(asset_uri, local_asset_path)
        return local_asset_path

    def get_asset(self, file_name):
        """
        - Use S3UriManager to get asset URI, asset_uri
        - Use VolumeManager to get local path, local_path
        - Use S3Transport to download(asset_uri, local_path)
        - Return local_path

        NOTE:
            Assets are files uploaded at launch-time, accessible read-only by any task during the run.
            If a task wants to write an asset, for use in a dependent task, it should use the Results sub-system.
        """

        print('Looking up asset: %s' % file_name)

        local_asset_uri = self.s3_uri_manager.uri_for_asset(file_name)

        print('Local asset uri: %s' % local_asset_uri)

        print('Testing exists')
        if s3_uri_exists(local_asset_uri):
            return self.__get_asset(local_asset_uri, file_name)
        print('Didnt exist locally')

        remote_asset_uri = self.container_services.remote_asset_uri(file_name)

        print('Remote asset uri: %s' % remote_asset_uri)

        print('Testing if its in map')
        if remote_asset_uri:
            print('Is in map')
            print('Testing object exists at URI')
            if s3_uri_exists(remote_asset_uri):
                return self.__get_asset(remote_asset_uri, file_name)

        raise ValueError('Unable to lookup asset, {}, locally or remotely'.format(file_name))

    ## Input ##

    def get_my_input_line(self):
        my_stage_name = self.container_services.get_curr_stage()
        return self.__get_input_line_for_stage(my_stage_name)

    def __get_input_line_for_stage(self, stage_name):
        # Can use the Pipeline spec to determine Aggregate, then from aggregate get correct Input
        # Then download/stream the manifest and return the line

        # If not an Array job, raise an error
        if not self.container_services.is_array_job(stage_name):
            raise ValueError('No input to get: Non-Array Aggregate')

        manifest_s3_uri = self.container_services.get_input_manifest_for_stage(stage_name)
        worker_id = int(self.container_services.get_worker_id())

        my_line = None
        item_idx = 0

        with smart_open(manifest_s3_uri, 'r') as manifest_fh:
            for line in manifest_fh:
                if not line.startswith(INPUT_MANIFEST_COMMENT_CHAR):
                    if item_idx == worker_id:
                        print('Found:', line)
                        my_line = line
                        break
                    else:
                        item_idx += 1

        if not my_line:
            raise ValueError('Unable to find line for worker in Input')
        else:
            return my_line

    @contextmanager
    def open_manifest(self, stage_name):

        manifest_url = self.container_services.get_input_manifest_for_stage(stage_name)

        def manifest_byte_decode_and_strip():
            with smart_open(manifest_url) as manifest:
                for line in manifest:
                    yield line.decode('UTF-8').strip()

        yield manifest_byte_decode_and_strip()

    @contextmanager
    def write_manifest(self, stage_name):

        print('Finding manifest for', stage_name)

        manifest_url = self.container_services.get_input_manifest_for_stage(stage_name)

        print('Manifest URL:', manifest_url)

        manifest_fh = smart_open(manifest_url, 'w')

        yield manifest_fh

        manifest_fh.close()

    ## Results ##

    def __count_items_in_array_stage(self, stage_name):

        if not self.container_services.is_array_job(stage_name):
            raise ValueError('No input to get: Non-Array Aggregate')

        manifest_s3_uri = self.container_services.get_input_manifest_for_stage(stage_name)

        num_items = 0

        with smart_open(manifest_s3_uri, 'r') as manifest_fh:
            for line in manifest_fh:
                if not line.startswith(INPUT_MANIFEST_COMMENT_CHAR):
                    num_items += 1

        return num_items

    def put_result_for_stage(self, stage_name, result_name, local_path):
        worker_id = self.container_services.get_worker_id() # can be None
        result_uri = self.s3_uri_manager.uri_for_result(stage_name, result_name, worker_id, local_path)
        self.s3_transport.upload(local_path, result_uri)
        self.dynamo_db.put_result(stage_name, result_name, worker_id, local_path, result_uri)

    def get_result_for_stage(self, stage_name, result_name):

        is_array_result = self.container_services.is_array_result(stage_name)

        # Two cases: Array result, or not
        item_count = self.__count_items_in_array_stage(stage_name) \
            if is_array_result else 1

        return Results(
            stage_name, result_name, self.container_services.get_worker_id(),
            item_count, is_array_result,
            self.dynamo_db, self.s3_transport, self.volume_path_manager
        )

    ## Execution ##

    def get_job_environment(self):
        return self.container_services.env_for_stage(
            self.container_services.get_curr_stage()
        )

    def __merge_substitutions(self, ext_subs):
        merged = dict()

        for dict_or_none in [self.get_job_environment(), ext_subs]:
            if dict_or_none:
                merged.update(dict_or_none)

        return merged

    def __run_cmd_from_string(self, cmd_string, cwd=None):
        print('Running: {}'.format(cmd_string))
        return subprocess.run(cmd_string, shell=True, check=True, cwd=cwd)

    def run_template(self, template, mappings, workdir=None):
        '''
        Builds merged placeholder-subs
        - subs {k->v}
        - stage/job/env {k->v}
        Subs the template
        '''
        merged_subs = self.__merge_substitutions(mappings)
        cmd_string = template.format(**merged_subs)
        completed_process = self.__run_cmd_from_string(cmd_string, cwd=workdir)
        completed_process.check_returncode()
        return completed_process

    def run_template_from_spec(self, mappings):
        '''
        Has some handle on the command spec.

        Gets template string like spec[name].
        Builds merged placeholder-subs
            - subs {k->v}
            - stage/job/env {k->v}
        Subs the template
        '''

        cmd_spec_local = self.get_asset(
            self.container_services.get_cmd_spec_name()
        )

        with open(cmd_spec_local) as spec:
            cmd_spec = yaml.safe_load(spec)

        stage_name = self.container_services.get_curr_stage()

        if stage_name in cmd_spec:
            template = cmd_spec[stage_name]
            return self.run_template(template, mappings)
        else:
            raise ValueError('No template found in {} for stage {}'.format(
                self.container_services.get_cmd_spec_name(), stage_name
            ))


class Results(Sequence):

    def __init__(self, stage_name, result_name, worker_id, num_items, is_array_result, dynamo_db, s3_transport, volume_path_manager):
        self.stage_name = stage_name
        self.result_name = result_name
        self.worker_id = worker_id
        self.num_items = num_items
        self.is_array_result = is_array_result
        self.dynamo_db = dynamo_db
        self.s3_transport = s3_transport
        self.volume_path_manager = volume_path_manager

    def get_array_result(self):

        if not self.worker_id:
            raise ValueError('Not an array task')

        result = self.dynamo_db.get_result(
            stage_name=self.stage_name,
            result_name=self.result_name,
            worker_id=self.worker_id
        )

        return self.__resultobj(result, self.worker_id)

    def __getitem__(self, item):
        # Go from number (item) to Hash(stage_name, result_name, worker_id)
        # Use Hash to GetItem

        if item >= self.num_items:
            raise IndexError()

        result = self.dynamo_db.get_result(
            stage_name=self.stage_name,
            result_name=self.result_name,
            worker_id=item if self.is_array_result else None
        )

        return self.__resultobj(result, item)

    def __resultobj(self, result_ddb, item_or_workerid):
        result_s3_uri = result_ddb['S3Uri']
        file_path = result_ddb['LocalPath']

        file_name = Path(urlparse(result_s3_uri).path).name
        dl_path = self.volume_path_manager.path_for_result(self.stage_name, self.result_name, str(item_or_workerid), file_name)

        return Result(result_s3_uri, dl_path, file_path, self.s3_transport)

    def __len__(self):
        return self.num_items


class Result:

    def __init__(self, s3_uri, dl_path, file_path, s3_transport):
        self.s3_uri = s3_uri
        self.dl_path = dl_path
        self.file_path = file_path
        self.s3_transport = s3_transport

    def get_initial_path(self):
        return self.file_path

    def download_to_volume(self):
        self.s3_transport.download(self.s3_uri, self.dl_path)
        return self.dl_path

    def remove_from_volume(self):
        p = Path(self.dl_path)
        if p.exists():
            p.unlink()


def pipeline_task(func):
    def volume_context_wrapper():
        data_layer = get_data_layer()
        try:
            data_layer.make_working_dir_in_volume()
            func(data_layer)
        finally:
            data_layer.remove_working_dir_in_volume()
    return volume_context_wrapper


def s3_url_unparse(bucket_name, key_name):
    return urlunparse(('s3', bucket_name, key_name, None, None, None))


def get_data_layer():

    # Note: Configuration system is not initialized in this context

    dynamo_db_resource = boto3.resource('dynamodb')
    s3_resource = boto3.resource('s3')

    container_services = ContainerServices()
    s3_transport = S3Transport(s3_resource)

    table_name = container_services.get_table_name()
    s3_base_uri = container_services.get_s3_base_uri()
    container_uid = container_services.next_container_uid()

    volume_path_manager = VolumePathManager(
        container_uid=container_uid,
        docker_volume_path=DOCKER_VOLUME_PATH_STR
    )

    share_path_manager = SharePathManager(share_volume_path=SHARE_VOLUME_PATH_STR)

    s3_uri_manager = S3UriManager(s3_base_uri)

    dynamo_db_facade = DynamoDbFacade(
        DynamoDb(
            table_name=table_name,
            dynamo_db_resource=dynamo_db_resource
        )
    )

    return DataLayer(
        dynamo_db=dynamo_db_facade,
        s3_transport=s3_transport,
        container_services=container_services,
        volume_path_manager=volume_path_manager,
        share_path_manager=share_path_manager,
        s3_uri_manager=s3_uri_manager
    )

