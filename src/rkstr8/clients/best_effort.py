import boto3
import datetime
import yaml
import json
import networkx as nx
from smart_open import smart_open
from abc import ABC, abstractmethod
from collections import defaultdict
from rkstr8.clients import Event, EventUnmarshaller, UnmarshallingContext, queue_id_for_stage
from rkstr8.clients.submitter import AggregateTypeDecider
from rkstr8.domain.pipeline import PipelineGraphBuilder
from rkstr8.cloud.batch import BatchJobListStatusPoller
from rkstr8.cloud.dynamodb import DynamoDb
from rkstr8.common.best_effort import DynamoDbFacade, AggregateNodeColoring, StageNodeColoring
from rkstr8.common.container import S3UriManager

COLORING = 'coloring'

WAITING = 'WAITING'
READY = 'READY'
SUBMITTED = 'SUBMITTED'
COMPLETED = 'COMPLETE'

STAT_SUCCEEDED = 'SUCCEEDED'
STAT_FAILED = 'FAILED'

EVENT_S3_URI_KEY = 'event_s3_uri'


def get_client(ref_event):

    value_event = value_event_from_ref_event(ref_event)

    context = UnmarshallingContext()
    event_api = Event(value_event)
    EventUnmarshaller(event_api, context).unmarshall_to_context()

    graph_builder = PipelineGraphBuilder()
    stage_graph = graph_builder.stage_graph(context.pipeline)
    aggregate_graph = graph_builder.aggregate_graph(context.pipeline, stage_graph)

    ddb = DynamoDbFacade(
        dynamo_db=DynamoDb(
            table_name=event_api.get_table_name(),
            dynamo_db_resource=boto3.resource('dynamodb')
        )
    )

    best_effort = BestEffort(event_api, context, aggregate_graph, stage_graph, ddb)

    return BestEffortClient(best_effort)


def value_event_from_ref_event(ref_event):

    # extract value event S3 URI from reference event
    event_s3_uri = ref_event[EVENT_S3_URI_KEY]

    # read the value event, unmarshall the yaml string
    with smart_open(event_s3_uri) as event_stream:
        return yaml.safe_load(event_stream)


class BestEffortClient:
    """
    Public API
    """
    def __init__(self, best_effort):
        self.best_effort = best_effort

    def schedule_one_round(self):
        return self.best_effort.schedule_one_round()


class Configuration:
    # Lookup aid, grouping aggregates by status

    def __init__(self, aggregate_graph):
        self.aggregate_graph = aggregate_graph
        self.config = defaultdict(list)
        self.__initialize_configuration()

    def __initialize_configuration(self):
        for aggregate in self.aggregate_graph:
            coloring = self.aggregate_graph.nodes[aggregate]['coloring']
            self.config[coloring.status].append(aggregate)

    def list_for(self, status):
        return self.config[status]


class BatchCloudwatchLogsPersister:

    # Invariants:
    #   Job is complete
    #   If Array Job, all sub-jobs are complete

    # TODO: Use Multiprocessing to handle array jobs in parrallel
    # https://aws.amazon.com/blogs/compute/parallel-processing-in-python-with-aws-lambda/

    # TODO: Decide on whether to use a CloudWath logs S3 export task in stead of traversing each stream
    # https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/S3ExportTasks.html
    # This would require a bucket policy, which cannot be done through CloudFormation

    def __init__(self, job_id, stage_name, batch_cli, logs_cli, s3_uri_manager):
        self.job_id = job_id
        self.stage_name = stage_name
        self.batch_cli = batch_cli
        self.logs_cli = logs_cli
        self.s3_uri_manager = s3_uri_manager

    def _is_array_job(self, describe_job_response):
        return 'arrayProperties' in describe_job_response['jobs'][0]

    def collect_and_persist_log_events(self):

        response = self.batch_cli.describe_jobs(
            jobs=[self.job_id]
        )

        # Instance could have been constructed with either Array or non-Array job.
        # Array sub-jobs are treated the same as non-Array jobs.
        # If an array job,

        if self._is_array_job(response):
            array_size = response['jobs'][0]['arrayProperties']['size']

            for idx in range(array_size):

                # Need to re-query for child...
                child_job_id = ':'.join((self.job_id, str(idx)))

                response = self.batch_cli.describe_jobs(
                    jobs=[child_job_id]
                )

                self._process_logging_job(response, array_job_idx=idx)
        else:
            self._process_logging_job(response)

    def _process_logging_job(self, describe_job_response, array_job_idx=None):
        # Gather log streams from all attempts
        # Precondition: Job is complete, e.g. either SUCCESS or FAIL, and there is at least one attempt
        # for each attempt

        for attempt_idx, attempt in enumerate(describe_job_response['attempts']):

            start_time_millis = attempt['container']['startedAt']
            stop_time_millis = attempt['container']['stoppedAt']

            start_str, stop_str, duration_str = self._process_attempt_duration(start_time_millis, stop_time_millis)

            log_stream_name = attempt['container']['logStreamName']

            self._persist_logs(log_stream_name, start_str, stop_str, duration_str, attempt_idx, array_job_idx)

    def _process_attempt_duration(self, start_time_millis, stop_time_millis):
        dt_start = datetime.datetime.fromtimestamp(start_time_millis//1000)
        dt_stop = datetime.datetime.fromtimestamp(stop_time_millis//1000)

        start_str = self.__datetime_to_str('Started', dt_start)
        stop_str = self.__datetime_to_str('Stopped', dt_stop)

        duration_seconds = (stop_time_millis - start_time_millis) // 1000
        duration_str = 'Duration: {elapsed}'.format(duration_seconds)

        return start_str, stop_str, duration_str

    def __datetime_to_str(self, prefix, dt):
        return '{prefix}: {month}/{day}/{year}: {hour}:{min}:{sec}'.format(
            prefix=prefix,
            month=dt.month,
            day=dt.day,
            year=dt.year,
            hour=dt.hour,
            min=dt.minute,
            sec=dt.second
        )

    def _persist_logs(self, log_stream_name, start_str, stop_str, duration_str, attempt_idx, array_job_idx=None):

        s3_log_uri = self.s3_uri_manager.uri_for_log(self.stage_name, attempt_idx, array_job_idx)

        with smart_open(s3_log_uri, 'w') as s3_log_fh:
            s3_log_fh.write(start_str + '\n')
            s3_log_fh.write(stop_str + '\n')
            s3_log_fh.write(duration_str + '\n')
            s3_log_fh.write('\n')
            self._process_log_stream(log_stream_name, s3_log_fh)

    def _process_log_stream(self, log_stream_name, s3_out_fh):

        kwargs = {
            'logGroupName': '/aws/batch/job',
            'logStreamName': log_stream_name,
            'startFromHead': True
        }

        while True:
            response = self.logs_cli.get_log_events(**kwargs)

            for event in response['events']:
                s3_out_fh.write(': '.join((event['timestamp'], event['message'])) + '\n')

            if len(response['events']) == 0:
                break

            prev_forward_token = response['nextForwardToken']

            kwargs = {
                'logGroupName': '/aws/batch/job',
                'logStreamName': log_stream_name,
                'nextToken': prev_forward_token
            }


class BestEffort:

    def __init__(self, event_api, event_context, aggregate_graph, stage_graph, dynamo_db_facade):
        self.event_api = event_api
        self.event_context = event_context
        self.aggregate_graph = aggregate_graph
        self.stage_graph = stage_graph
        self.dynamo_db_facade = dynamo_db_facade
        self._fail_encountered = False

    def _load_snapshot(self):
        # This function mutates self.stage_graph and self.aggregate_graph in place by coloring nodes
        print('Loading snapshot')

        # Load Aggregate colorings and apply to graph
        for aggregate in self.aggregate_graph:
            coloring = self.dynamo_db_facade.get_agg_coloring(aggregate.name)
            self.aggregate_graph.nodes[aggregate]['coloring'] = coloring
            print('{}: {}'.format(aggregate.name, coloring.status))

        # Load stage colorings and apply to graph
        for stage in self.stage_graph:
            coloring = self.dynamo_db_facade.get_stage_coloring(stage.name)
            self.stage_graph.nodes[stage]['coloring'] = coloring
            print('{}: {},{}'.format(stage.name, str(coloring.job_id), str(coloring.queue_id)))

    def _complete(self):
        print('Polling SUBMITTED for FAIL/SUCCESS...')

        # For each aggregate with status SUBMITTED, poll it, on complete:
        #   - if there exists a stage with status FAILED: self._fail_encountered = True
        #   - if all jobs exited SUCCEEDED
        #       - change aggregate color to status COMPLETED
        configuration = Configuration(self.aggregate_graph)
        for aggregate in configuration.list_for(SUBMITTED):
            # Get job_id color from each stage in aggregate, gather into list
            aggregate_job_ids = []
            for stage in aggregate.stages:
                stage_coloring = self.stage_graph.nodes[stage]['coloring']
                job_id = stage_coloring.job_id
                if not job_id:
                    raise ValueError('Every stage should have a job_id color if agg submitted.')
                aggregate_job_ids.append(job_id)
            # Poll the list
            poller = BatchJobListStatusPoller(aggregate_job_ids)
            outcome = poller.polling_outcome()

            if outcome == 'SUCCESS':
                # Re-color Agg to status COMPLETED
                coloring = self.aggregate_graph.nodes[aggregate]['coloring']
                coloring.status = COMPLETED
            elif outcome == 'FAIL':
                self._fail_encountered = True
            elif outcome == 'IN_PROGRESS':
                pass
            else:
                ValueError('Unexpected polling outcome:{}'.format(outcome))

    def _promote(self):
        print('Promoting Waiting to Ready...')

        configuration = Configuration(self.aggregate_graph)
        for aggregate in configuration.list_for(WAITING):

            # Test indegree == 0
            if self.aggregate_graph.in_degree(aggregate) == 0:
                print('Promoting {}'.format(aggregate.name))
                self.aggregate_graph.nodes[aggregate]['coloring'].status = READY
            else:
                # Test predecessor completion
                predecessors = self.aggregate_graph.predecessors(aggregate)
                pred_statuses = [self.aggregate_graph.nodes[pred]['coloring'].status for pred in predecessors]
                if set(pred_statuses) == {COMPLETED}:
                    self.aggregate_graph.nodes[aggregate]['coloring'].status = READY

    def _submit(self):
        print('Submitting any Ready tasks...')

        configuration = Configuration(self.aggregate_graph)
        for aggregate in configuration.list_for(READY):
            submitter = agg_type_submitter_factory(aggregate, self.event_api, self.event_context)

            stage_result_map = submitter.submit()

            for stage, response in stage_result_map.items():
                try:
                    job_id = response['jobId']
                    print('Found jobId: %s' % job_id)
                except KeyError:
                    raise ValueError('Failed to find jobId key in submit response: {}'.format(response))
                else:
                    print('Coloring stage...')
                    self.stage_graph.nodes[stage]['coloring'].job_id = job_id
                    self.stage_graph.nodes[stage]['coloring'].queue_id = queue_id_for_stage(self.event_api, stage)

            print('Coloring agg...')
            self.aggregate_graph.nodes[aggregate]['coloring'].status = SUBMITTED

    def _store_snapshot(self):
        print('Storing snapshot in DynamoDb')
        # Load Aggregate colorings and apply to graph
        for aggregate in self.aggregate_graph:
            coloring = self.aggregate_graph.nodes[aggregate]['coloring']
            self.dynamo_db_facade.put_agg_coloring(aggregate.name, coloring)

        # Load stage colorings and apply to graph
        for stage in self.stage_graph:
            coloring = self.stage_graph.nodes[stage]['coloring']
            self.dynamo_db_facade.put_stage_coloring(stage.name, coloring)

    def _update_status(self):
        print('Updating status based on Complete...')

        if self._fail_encountered:
            status = 'FAIL'
        else:
            # SUCCESS if all aggs STATUS == COMPLETED this implies all jobs therein STATUS == SUCCESS
            #   (O.w. self._fail_encountered would have been set)
            configuration = Configuration(self.aggregate_graph)
            completed_aggs = set(configuration.list_for(COMPLETED))
            all_aggs = set(self.aggregate_graph.nodes)

            if completed_aggs == all_aggs:
                status = 'SUCCESS'
            elif completed_aggs.issubset(all_aggs):
                status = 'IN_PROGRESS'
            else:
                raise ValueError('Completed aggs must be a subset of all aggs.')

        return status

    def schedule_one_round(self):
        self._load_snapshot()
        self._complete()
        self._promote()
        self._submit()
        self._store_snapshot()
        return self._update_status()


def agg_type_submitter_factory(agg, event_api, event_context):
    agg_type = agg.type
    batch_client = boto3.client('batch')

    if agg_type == 'array':
        return ArraySubmitter(
            aggregate=agg,
            batch_client=batch_client,
            event_api=event_api,
            graph_builder=PipelineGraphBuilder(),
            event_context=event_context
        )
    elif agg_type == 'simple':
        return SimpleSubmitter(
            aggregate=agg,
            batch_client=batch_client,
            event_api=event_api
        )
    elif agg_type == 'concurrent':
        return ConcurrentSubmitter(
            aggregate=agg,
            batch_client=batch_client,
            event_api=event_api
        )


class Submitter(ABC):

    def __init__(self, batch_client, event_api):
        self.batch_client = batch_client
        self.event_api = event_api
        self.uri_manager = S3UriManager(s3_base_uri=self.event_api.get_s3_base_uri())

    @abstractmethod
    def submit(self):
        pass

    def ref_event_for_val_event(self, stage_name, event_api):

        # Get an S3URI for the value event for this job
        event_s3_uri = self.uri_manager.uri_for_event(label=stage_name)

        # Upload the value event to S3URI
        with smart_open(event_s3_uri, 'w') as event_stream:
            event_stream.write(
                yaml.safe_dump(event_api.event)
            )

        # Return the reference event
        return {
            EVENT_S3_URI_KEY: event_s3_uri
        }

    def submit_job(self, stage_name, job_def_id, queue_id, event_str):

        response = JobSubmissionRequest(self.batch_client, self.event_api)\
            .set_job_name_suffixed(stage_name) \
            .set_job_def_id(job_def_id) \
            .set_queue_id(queue_id) \
            .set_event_str(event_str) \
            .submit()

        return response


class ArraySubmitter(Submitter):

    JOB_REQUEST = 'job_request'
    JOB_ID = 'job_id'

    def __init__(self, aggregate, batch_client, event_api, graph_builder, event_context):
        super().__init__(batch_client, event_api)
        self.aggregate = aggregate
        self.batch_client = batch_client
        self.graph_builder = graph_builder
        self.event_context = event_context

    def _count_input_items(self, curr_input):
        num_items = 0
        with smart_open(curr_input, 'r') as stream:
            for _ in stream:
                    num_items += 1
        return num_items

    def _build_submittable_graph(self, pipeline, aggregate):

        print('_build_submittable_graph')

        # build stage graph for entire pipeline (all aggregates)
        total_stage_graph = self.graph_builder.stage_graph(pipeline)

        print('total_stage_graph', total_stage_graph.edges)

        # slice out subgraph for current aggregate
        agg_subgraph = nx.subgraph(total_stage_graph, aggregate.stages)

        print('agg_subgraph', agg_subgraph.edges)

        # annotate each node with a JobSubmissionRequest, and empty job_id, to be assigned during submission scope
        for stage in aggregate.stages:
            agg_subgraph.nodes[stage][ArraySubmitter.JOB_REQUEST] = JobSubmissionRequest(self.batch_client, self.event_api)
            agg_subgraph.nodes[stage][ArraySubmitter.JOB_ID] = ''

        return agg_subgraph

    def _submitting_traversal(self, graph, num_items, job_def_ids, event):

        stage_response_map = dict()

        for stage in nx.topological_sort(graph):

            print('Visiting node: %s' % stage.name)

            print('Updating to {}....'.format(stage.name))
            event.set_curr_stage(stage.name)
            print('Event curr stage: {}'.format(event.get_curr_stage()))

            ref_event = self.ref_event_for_val_event(stage.name, event)
            event_str = json.dumps(ref_event)

            queue_id = queue_id_for_stage(event, stage)

            job_request = graph.nodes[stage][ArraySubmitter.JOB_REQUEST]

            job_request \
                .set_job_name_suffixed(stage.name) \
                .set_queue_id(queue_id) \
                .set_job_def_id(job_def_ids[stage.name]) \
                .set_event_str(event_str) \
                .add_environment_var('ARRAY_SIZE', str(num_items))

            if num_items > 1:
                # set_array_properties(int) marks this job as an array job
                #  "The array size can be between 2 and 10,000. If you specify array properties for a job, it becomes
                #  an array job." https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html#Batch.Client.submit_job
                job_request.set_array_properties(num_items)

            for predecessor in graph.predecessors(stage):
                print('Adding predecessor: %s' % predecessor.name)
                parent_job_id = graph.nodes[predecessor][ArraySubmitter.JOB_ID]
                if num_items > 1:
                    job_request.add_dependency(parent_job_id, 'N_TO_N')
                elif num_items == 0:
                    job_request.add_dependency(parent_job_id)
                else:
                    raise ValueError('')

            print('Submitting...')
            response = job_request.submit()

            try:
                job_id = response['jobId']
                print('Found jobId: %s' % job_id)
            except KeyError:
                raise ValueError('Failed to find jobId key in submit response: {}'.format(response))
            else:
                graph.nodes[stage][ArraySubmitter.JOB_ID] = job_id
                stage_response_map[stage] = response

        return stage_response_map

    def submit(self):

        # curr_stage_name = self.event_api.get_curr_stage()
        # curr_stage = pipeline.get_stage(curr_stage_name)
        # curr_aggregate = pipeline.get_aggregate_for_stage(curr_stage)
        # curr_queue_id = queue_id_for_stage(self.event_api, curr_stage)

        print('ArraySubmitter.submit', self.aggregate.name)

        pipeline = self.event_context.pipeline

        input_ = pipeline.get_input(self.aggregate.name)

        print('Input.manifest', input_.manifest)

        num_items = self._count_input_items(input_.manifest)

        print('Num items', num_items)

        graph = self._build_submittable_graph(pipeline, self.aggregate)

        job_def_ids = self.event_api.get_job_definition_ids()

        return self._submitting_traversal(graph, num_items, job_def_ids, self.event_api)


class ConcurrentSubmitter(Submitter):

    def __init__(self, aggregate, batch_client, event_api):
        super().__init__(batch_client, event_api)
        self.aggregate = aggregate

    def submit(self):
        return {stage: self._submit_stage(stage) for stage in self.aggregate.stages}

    def _submit_stage(self, stage):

        stage_name = stage.name
        queue_id = queue_id_for_stage(self.event_api, stage)
        job_def_id = self.event_api.get_job_definition_ids()[stage_name]

        self.event_api.set_curr_stage(stage_name)
        ref_event = self.ref_event_for_val_event(stage_name, self.event_api)
        event_str = json.dumps(ref_event)

        return self.submit_job(stage_name, job_def_id, queue_id, event_str)


class SimpleSubmitter(Submitter):

    def __init__(self, aggregate, batch_client, event_api):
        super().__init__(batch_client, event_api)
        self.aggregate = aggregate

    def submit(self):

        print('SingleBatchJobSubmitter.submit_job')

        stage = self.aggregate.stages[0]

        stage_name = stage.name
        queue_id = queue_id_for_stage(self.event_api, stage)
        job_def_id = self.event_api.get_job_definition_ids()[stage_name]

        self.event_api.set_curr_stage(stage_name)
        ref_event = self.ref_event_for_val_event(stage_name, self.event_api)
        event_str = json.dumps(ref_event)

        return {stage: self.submit_job(stage_name, job_def_id, queue_id, event_str)}


class JobSubmissionRequest:

    def __init__(self, batch_client, event_api):
        self.batch_client = batch_client
        self.event_api = event_api

        self.request_params = {}

    def job_name_from_prefix(self, job_name_prefix):
        now = datetime.datetime.now()
        return '{}_{}_{}_{}'.format(job_name_prefix, now.hour, now.minute, now.second)

    def set_array_properties(self, num_items):
        self.request_params['arrayProperties'] = {
            'size': num_items
        }
        return self

    def add_dependency(self, job_id, type_=None):
        depends_on_key = 'dependsOn'

        if depends_on_key not in self.request_params:
            self.request_params[depends_on_key] = []

        dependency = {
            'jobId': job_id,
        }

        if type_:
            dependency['type'] = type_

        self.request_params[depends_on_key].append(dependency)

        return self

    def set_job_name_suffixed(self, job_name_prefix):
        self.request_params['jobName'] = self.job_name_from_prefix(job_name_prefix)
        return self

    def set_queue_id(self, queue_id):
        self.request_params['jobQueue'] = queue_id
        return self

    def set_job_def_id(self, job_def_id):
        self.request_params['jobDefinition'] = job_def_id
        return self

    def set_event_str(self, event_str):
        print('set_event_str')

        var_name = self.event_api.get_event_env_var_name()
        var_value = event_str

        print('name', var_name)
        print('value', var_value)

        self.add_environment_var(name=var_name, value=var_value)
        return self

    def add_environment_var(self, name, value):
        cont_over_key = 'containerOverrides'
        environment_key = 'environment'

        if cont_over_key not in self.request_params:
            self.request_params[cont_over_key] = {
                environment_key: []
            }

        self.request_params[cont_over_key][environment_key].append(
            {
                'name': name,
                'value': value
            }
        )

        return self

    def _validate_request(self):
        request_keys = list(self.request_params.keys())

        if any(key not in request_keys for key in ('jobName', 'jobQueue', 'jobDefinition', 'containerOverrides')):
            raise Exception('Must set essential variables before submitting job.')

        try:
            overrides = self.request_params['containerOverrides']
            environment = overrides['environment']
        except KeyError:
            raise Exception('Must set essential variables before submitting job.')
        else:
            print(yaml.dump(self.request_params, default_flow_style=False))

            matches = [name_value_pair for name_value_pair in environment if name_value_pair['name'] == self.event_api.get_event_env_var_name()]
            if len(matches) != 1:
                raise Exception('Must pass event as {} in containerOverrides.'.format(self.event_api.get_event_env_var_name()))

    def submit(self):
        self._validate_request()

        print('> Submitting job:')
        print(yaml.dump(self.request_params))

        response = self.batch_client.submit_job(
            **self.request_params
        )

        print('> Response:')
        print(yaml.dump(response))

        return response
