import json
import networkx as nx
from rkstr8.domain.pipeline import PipelineFactory


def queue_id_for_stage(event_api, stage):
    matches = [queue_id for queue_name, queue_id in event_api.get_queue_ids().items() if queue_name == stage.queue_name]
    try:
        queue_id = matches.pop()
    except IndexError:
        raise ValueError('Could not find queue in {} for queue named, {}'.format(event_api.get_queue_ids(), stage.queue_name))
    else:
        return queue_id


class UnmarshallingContext:
    '''
    Holds (only) unmarshalled objects from their serialized forms on the input event
    '''

    def __init__(self):
        self.pipeline = None
        self.job_definitions = None


class Event:
    """
    Facade for interfacing with the json-serialized event
    """

    def __init__(self, event):
        self.event = event

    def get_curr_stage(self):
        runtime = self._get_runtime()
        return runtime['curr_stage']

    def set_curr_stage(self, stage_name):
        runtime = self._get_runtime()
        runtime['curr_stage'] = stage_name

    def get_event_env_var_name(self):
        batch = self._get_batch()
        return batch['EventEnvVarName']

    def get_table_name(self):
        dynamo_db = self._get_dynamo_db()
        return dynamo_db['table_name']

    def get_s3_base_uri(self):
        meta_data = self._get_meta_data()
        return meta_data['s3_base']

    def get_cmd_spec_name(self):
        meta_data = self._get_meta_data()
        return meta_data['cmd_tmpl_file']

    def get_s3_remote_files(self):
        meta_data = self._get_meta_data()
        return meta_data['s3_remote_files']

    def get_pipeline_spec(self):
        return self.event['Pipeline']

    def get_job_definition_ids(self):
        batch = self._get_batch()
        return batch['JobDefinitions']

    def get_queue_ids(self):
        batch = self._get_batch()
        return batch['JobQueues']

    def to_json(self):
        return json.dumps(self.event)

    def _get_meta_data(self):
        return self.event['MetaData']

    def _get_dynamo_db(self):
        return self.event['DynamoDb']

    def _get_batch(self):
        return self.event['Batch']

    def _get_runtime(self):
        return self.event['Runtime']


class EventUnmarshaller:
    '''
    Unmarshalls serialized objects from event, storing them in unmarshalling_context
    '''

    def __init__(self, event, unmarshalling_context):
        self.event = event
        self.unmarshalling_context = unmarshalling_context

    def _pipeline_from_spec(self):
        factory = PipelineFactory()
        return factory.pipeline_from(
            struct_or_path= {'Pipeline': self.event.get_pipeline_spec()}
        )

    def unmarshall_to_context(self):

        pipeline = self._pipeline_from_spec()
        job_definitions = self.event.get_job_definition_ids()

        self.unmarshalling_context.pipeline = pipeline
        self.unmarshalling_context.job_definitions = job_definitions

        # curr_stage = pipeline.get_stage(self.event.get_stage_name())
        # curr_aggregate = pipeline.get_aggregate_for_stage(curr_stage)
        # curr_queue = pipeline.get_queue(self.event.get_queue_name())
        # curr_queue_id = self._queue_id_for_stage(curr_stage)
        # curr_input = pipeline.get_input(curr_aggregate.name)
        # nx.Digraph where nodes are Aggregates

        # print(pipeline.stages)
        # print(execution_plan.edges)
        # print(job_definitions)