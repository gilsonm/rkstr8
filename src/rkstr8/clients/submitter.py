from abc import ABC, abstractmethod
import boto3
import networkx as nx
import datetime
import yaml
from smart_open import smart_open
from rkstr8.domain.pipeline import PipelineGraphBuilder
from rkstr8.cloud import Command
from rkstr8.clients import EventUnmarshaller, UnmarshallingContext, Event, queue_id_for_stage
from rkstr8.conf import BatchConfig

INPUT_MANIFEST_COMMENT_CHAR = '#'

# Public API ###################################################################


def get_submitter(event):
   return Submitter(
       event,
       UnmarshallingContext(),
       boto3.client('batch'),
   )


class Submitter:

    def __init__(self, event, unmarshalling_context, batch_client):
        self.event = event
        self.event_api = None

        self.unmarshalling_context = unmarshalling_context
        self.batch_client = batch_client

    def __conditional_event_updates(self):
        """
        Pre-conditions:
          1. self.event_api is set
          2. self.unmarshalling_context is set from unmarshalling the event_api

        Post-conditions:
          0. Pre-conditions still hold
          1. event['Runtime']['is_first_stage'] == False
          2. event['Runtime']['curr_stage'] == updater(curr_stage) --> next_stage
          3. event['Runtime']['curr_queue'] == next_stage.queue_name
        """
        if self.event_api.is_first_stage():
            pass
        else:
            self.__update_stage()

        self.event_api.set_is_first_stage(False)

    def _initialize_if_needed(self):

        if not self.event_api:
            self.event_api = Event(self.event)
            EventUnmarshaller(self.event_api, self.unmarshalling_context).unmarshall_to_context()
            self.__conditional_event_updates()

    def get_submission_action_and_context(self):

        self._initialize_if_needed()

        decider = AggregateTypeDecider(
            unmarshalling_context=self.unmarshalling_context,
            event_api=self.event_api,
            batch_client=self.batch_client
        )

        submission_action = decider.submission_action_for_event()

        return submission_action, self.unmarshalling_context

    def get_updated_event(self):

        self._initialize_if_needed()

        return self.event_api.event

    def __update_stage(self):

        linear_updater = LinearUpdater(
            pipeline=self.unmarshalling_context.pipeline,
            pipeline_graph_builder=PipelineGraphBuilder(),
            aggregate_execution_graph=self.unmarshalling_context.execution_plan
        )

        next_stages = linear_updater.next_stages(
            curr_stage_name=self.event_api.get_curr_stage()
        )

        try:
            next_stage = next_stages.pop()
        except IndexError:
            # No next stage. This branch should not execute during normal execution.
            next_stage = 'END'

        self.event_api.set_curr_stage(next_stage.name)

        return next_stage.name

    def update_branch(self, event):
        # self._unmarshall_if_needed()
        raise NotImplementedError('Not implemented until Parallel/Concurrent aggregates are implemented.')


class Updater(ABC):

    @abstractmethod
    def first_aggregates(self, aggregate_execution_graph):
        pass

    @abstractmethod
    def next_stages(self, curr_stage_name):
        pass

    @abstractmethod
    def roots_of_aggregate(self, aggregate):
        pass


class LinearUpdater(Updater):

    def __init__(self, pipeline, pipeline_graph_builder, aggregate_execution_graph):
        self.pipeline = pipeline
        self.pipeline_graph_builder = pipeline_graph_builder

        self.aggregate_execution_graph = aggregate_execution_graph

    def first_aggregates(self, aggregate_execution_graph):
        topo_list = list(nx.topological_sort(aggregate_execution_graph))
        topo_list.reverse()
        return [topo_list.pop()]

    def roots_of_aggregate(self, aggregate):
        stage_graph = self.pipeline_graph_builder.stage_graph(self.pipeline)
        agg_subgraph = nx.subgraph(stage_graph, aggregate.stages)
        topo_list = list(nx.topological_sort(agg_subgraph))
        topo_list.reverse()
        a_root = topo_list.pop()
        return [a_root]

    def next_stages(self, curr_stage_name):
        print('LinearUpdater.next_stages(...)')
        print(curr_stage_name)

        # 1. Stage object from curr_stage_name using rkstr8.domain.pipeline.Pipeline.get_stage(..)
        curr_stage = self.pipeline.get_stage(curr_stage_name)
        print(curr_stage.name)

        # 2. Find curr_aggregate from Stage -> Aggregate Pipeline.get_aggregate_for_stage(Stage ..)
        curr_aggregate = self.pipeline.get_aggregate_for_stage(curr_stage)
        print(curr_aggregate.name)

        # 3. Identify next Aggregate from curr_aggregate
        try:
            next_aggregate = list(self.aggregate_execution_graph.successors(curr_aggregate)).pop()
            print('Next agg: {}'.format(next_aggregate.name))
        except IndexError:
            print('> IndexError')
            # 3a. curr_aggregate is last aggregate.
            return []

        # 4. Using StageGraph, within next_aggregate find starting stage
        next_stages = self.roots_of_aggregate(next_aggregate)
        print('Next stages: {}'.format(next_stages))

        # 5. Return that stage
        #
        # NOTE: Maybe do transitive reduction on aggregate_execution_graph, considering multiple out_edges
        #       to multiple, distinct successor aggregates. Not sure this solves it still for all cases.
        return next_stages


class SubmissionActionDecider(ABC):
    """
    Takes event/Runtime/Pipeline and returns necessary stuff as class hierarchy?

    This is partly a factory
    """
    def __init__(self, unmarshalling_context, event_api, batch_client):
        self.unmarshalling_context = unmarshalling_context
        self.event_api = event_api
        self.batch_client = batch_client

    @abstractmethod
    def submission_action_for_event(self):
        pass


class AggregateTypeDecider(SubmissionActionDecider):

    def __init__(self, unmarshalling_context, event_api, batch_client):
        super().__init__(unmarshalling_context, event_api, batch_client)

    def submission_action_for_event(self):
        """
        This thing factories up the Pipeline, graph for ExecutionPlan (over agg_names or Aggregates?),
        curr_stage as Stage, curr_queue as Queue passes those on

        :return: ArraySubmissionAction|SimpleSubmissionAction
        """

        curr_stage_name = self.event_api.get_curr_stage()
        print('Decider> Got this stage name: ' + curr_stage_name)

        print('Decider> Can see these stages in Pipeline: {}'.format(self.unmarshalling_context.pipeline.stages))

        curr_stage = self.unmarshalling_context.pipeline.get_stage(curr_stage_name)
        print('Decider> Got this Stage: ' + str(curr_stage))

        # None
        curr_aggregate = self.unmarshalling_context.pipeline.get_aggregate_for_stage(curr_stage)
        print(curr_aggregate)

        agg_type = curr_aggregate.type

        if agg_type == 'array':
            submission_action = GraphSubmissionAction(
                submitter=GraphBatchJobSubmitter(self.batch_client, PipelineGraphBuilder(), self.event_api)
            )
        elif agg_type == 'simple':
            submission_action = SimpleSubmissionAction(
                submitter=SingleBatchJobSubmitter(self.batch_client, self.event_api)
            )
        else:
            submission_action = None

        return submission_action


class SubmissionAction(Command):

    @abstractmethod
    def execute(self, context):
        pass


class SingleBatchJobSubmitter:

    def __init__(self, batch_client, event_api):
        self.request = JobSubmissionRequest(batch_client, event_api)
        self.event_api = event_api

    def submit_job(self, pipeline):

        print('SingleBatchJobSubmitter.submit_job')

        stage_name = self.event_api.get_curr_stage()
        queue_id = queue_id_for_stage(self.event_api, pipeline.get_stage(stage_name))
        job_def_id = self.event_api.get_job_definition_ids()[stage_name]
        event_str = self.event_api.to_json()

        response = self.request \
            .set_job_name_suffixed(stage_name) \
            .set_queue_id(queue_id) \
            .set_job_def_id(job_def_id) \
            .set_event_str(event_str) \
            .submit()

        return response


class SimpleSubmissionAction(SubmissionAction):

    def __init__(self, submitter: SingleBatchJobSubmitter):
        self.submitter = submitter

    def execute(self, context):
        print('SimpleSubmissionAction')
        return self.submitter.submit_job(context.pipeline)


class GraphSubmissionAction(SubmissionAction):

    def __init__(self, submitter):
        self.submitter = submitter

    def execute(self, context):

        pipeline = context.pipeline
        self.submitter.submit_jobs(pipeline)


class GraphBatchJobSubmitter:

    JOB_REQUEST = 'job_request'
    JOB_ID = 'job_id'

    def __init__(self, batch_client, graph_builder, event_api):
        self.batch_client = batch_client
        self.graph_builder = graph_builder
        self.event_api = event_api

    def _count_input_items(self, curr_input):
        # Manifest file format should be: #=comment, it's a TSV, and one Item per non-comment line
        num_items = 0
        with smart_open(curr_input, 'r') as stream:
            for line in stream:
                if not line.startswith(INPUT_MANIFEST_COMMENT_CHAR):
                    num_items += 1

        return num_items

    def _build_submittable_graph(self, pipeline, curr_aggregate):

        # build stage graph for entire pipeline (all aggregates)
        total_stage_graph = self.graph_builder.stage_graph(pipeline)

        # slice out subgraph for current aggregate
        agg_subgraph = nx.subgraph(total_stage_graph, curr_aggregate.stages)

        # annotate each node with a JobSubmissionRequest, and empty job_id, to be assigned during submission scope
        for stage in curr_aggregate.stages:
            agg_subgraph.nodes[stage][GraphBatchJobSubmitter.JOB_REQUEST] = JobSubmissionRequest(self.batch_client, self.event_api)
            agg_subgraph.nodes[stage][GraphBatchJobSubmitter.JOB_ID] = ''

        return agg_subgraph

    def _submitting_traversal(self, graph, num_items, queue_id, job_def_ids, event):

        for stage in nx.topological_sort(graph):

            print('Visiting node: %s' % stage.name)

            # TODO: Bug. This will submit same curr_stage in event to each distinct stage in graph
            # TODO: Fix
            #       Update stage in event (or copy thereof)
            print('Event curr stage: {}'.format(event.get_curr_stage()))
            print('Updating to {}....'.format(stage.name))
            event.set_curr_stage(stage.name)
            print('Event curr stage: {}'.format(event.get_curr_stage()))

            job_request = graph.nodes[stage][GraphBatchJobSubmitter.JOB_REQUEST]

            job_request \
                .set_job_name_suffixed(stage.name) \
                .set_queue_id(queue_id) \
                .set_job_def_id(job_def_ids[stage.name]) \
                .set_array_properties(num_items) \
                .set_event_str(event.to_json())

            for predecessor in graph.predecessors(stage):
                print('Adding predecessor: %s' % predecessor.name)
                parent_job_id = graph.nodes[predecessor][GraphBatchJobSubmitter.JOB_ID]
                job_request.add_dependency(parent_job_id, 'N_TO_N')

            print('Submitting...')
            response = job_request.submit()

            try:
                job_id = response['jobId']
                print('Found jobId: %s' % job_id)
            except KeyError:
                raise ValueError('Failed to find jobId key in submit response: {}'.format(response))
            else:
                graph.nodes[stage][GraphBatchJobSubmitter.JOB_ID] = job_id

    def submit_jobs(self, pipeline):

        curr_stage_name = self.event_api.get_curr_stage()
        curr_stage = pipeline.get_stage(curr_stage_name)

        curr_aggregate = pipeline.get_aggregate_for_stage(curr_stage)
        curr_input = pipeline.get_input(curr_aggregate.name)
        curr_queue_id = queue_id_for_stage(self.event_api, curr_stage)
        job_def_ids = self.event_api.get_job_definition_ids()
        event = self.event_api

        num_items = self._count_input_items(curr_input.manifest)
        graph = self._build_submittable_graph(pipeline, curr_aggregate)
        self._submitting_traversal(graph, num_items, curr_queue_id, job_def_ids, event)


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

    def add_dependency(self, job_id, type_):
        depends_on_key = 'dependsOn'

        if depends_on_key not in self.request_params:
            self.request_params[depends_on_key] = []

        self.request_params[depends_on_key].append(
            {
                'jobId': job_id,
                'type': type_
            }
        )

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

        self._add_environment_var(name=var_name, value=var_value)
        return self

    def _add_environment_var(self, name, value):
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
