import yaml
import json
import re
import bitmath
import boto
import boto3
import docker
import subprocess
import shutil
import os.path
import time
import tempfile
import functools
import importlib.resources as importlib_resources
from rkstr8.conf import *
from dataclasses import dataclass
from botocore.exceptions import WaiterError
from arnparse import arnparse
from pprint import pprint
from boto.s3.key import Key
from urllib.parse import urlparse, urlunparse
from collections import defaultdict
from abc import ABC, abstractmethod
import networkx as nx
from rkstr8.cloud.stepfunctions import AsyncPoller, SingleTaskLoop, Task, Fail, Succeed, \
    StateMachine, States, State, flatten, wrap_cfn_variable
from rkstr8.cloud.cloudformation import TemplateProcessor
from rkstr8.cloud import BotoClientFactory, Service
from rkstr8.cloud.dynamodb import DynamoDb
from rkstr8.common.best_effort import DynamoDbFacade, AggregateNodeColoring, StageNodeColoring
from rkstr8.common.container import S3UriManager, S3Transport
from pathlib import Path
from smart_open import smart_open


# user names can only be snakecase
VALID_USER_NAMES = re.compile('[a-z]+(_[a-z]+)*')


def _valid_name(name):
    return VALID_USER_NAMES.fullmatch(name)


class PipelineGraphBuilder:

    def _is_connectedish(self, graph):
        return nx.is_connected(graph.to_undirected())

    def _name_to_stages(self, stages):
        return {
            stage.name: stage
            for stage in stages
        }

    def stage_graph(self, pipeline):
        stages = pipeline.stages
        name_to_stages = self._name_to_stages(stages)

        stage_graph = nx.DiGraph()

        stage_graph.add_nodes_from(stages)

        for stage in stages:
            next_stage_names = stage.next_list
            for next_stage_name in next_stage_names:
                next_stage = name_to_stages[next_stage_name]
                stage_graph.add_edge(stage, next_stage)

        print(stage_graph.edges)

        if not self._is_connectedish(stage_graph):
            raise ValueError('Expected a single connected component for stage graph.')

        return stage_graph

    # def _out_edges(self, ):

    def aggregate_graph(self, pipeline, stage_graph):

        aggregate_graph = nx.DiGraph()

        # Add all the aggregates to the graph so we can check single connected component
        # Otherwise, only nodes with edges would be added
        aggregates = pipeline.aggregates

        aggregate_graph.add_nodes_from(aggregates)

        # TODO: find edges between aggregates using stage_graph
        # Is an out-edge pointing out of aggregate?
        #   If yes, which aggregate is that stage part of?

        stage_to_aggr = {stage.name: aggregate for aggregate in aggregates for stage in aggregate.stages}

        # for each aggregate
        for aggregate in aggregates:
            stages = aggregate.stages
            # for each stage in that aggregate
            for stage in stages:
                # get names of successors
                successor_names = [successor_stage.name for successor_stage in stage_graph.successors(stage)]
                for successor_name in successor_names:
                    # get aggregate for stage
                    successor_aggregate = stage_to_aggr[successor_name]
                    if aggregate != successor_aggregate:
                        # No-op if edge exists
                        #
                        # Also, we don't care if there are multiple Stages that
                        #   have successors in a particular Aggregate.
                        #
                        # All stages of an aggregate must complete before a transition
                        #   to the next aggregate.
                        aggregate_graph.add_edge(aggregate, successor_aggregate)

        print(aggregates)
        print(aggregate_graph.edges)

        if not self._is_connectedish(aggregate_graph):
            raise ValueError('Expected a single connected component for aggregate graph.')

        return aggregate_graph


def dump_pipeline(pipeline):

    print('Cluster:')
    print('  min_cpus: ' + str(pipeline.cluster.min_cpus))
    print('  max_cpus: ' + str(pipeline.cluster.max_cpus))
    print('  des_cpus: ' + str(pipeline.cluster.desired_cpus))
    print('  instance_types: ' + str(pipeline.cluster.instance_types))

    print('Queues:')
    for q in pipeline.queues:
        print('  ' + q.name)

    print('Inputs:')
    for input_ in pipeline.inputs:
        print('  {}: {}'.format(input_.aggregate_name, input_.manifest))

    print('Stages:')
    for stage in pipeline.stages:
        print('  ' + stage.name)
        job = stage.job
        print('    image: ' + job.image)
        print('    mem: ' + str(job.memory))
        print('    cpus: ' + str(job.cpus))
        print('    cmd: ' + str(job.cmd_list))

    print('Aggregates:')
    for agg in pipeline.aggregates:
        print('  {}: {}'.format(agg.name, agg.type))
        for stage in agg.stages:
            print('    {}'.format(stage.name))


class UnmarshallYamlPipeline:

    def __init__(self, config: Config):
        self.pipeline_config = config.pipeline_config

    def unmarshall_pipeline(self, resolver):
        pipeline_path = Path(resolver(self.pipeline_config.pipeline_file))
        factory = PipelineFactory()
        pipeline = factory.pipeline_from(pipeline_path)
        dump_pipeline(pipeline)

        return pipeline


class PipelineFactory:

    def pipeline_from(self, struct_or_path):
        factory = self.get_pipeline_factory(struct_or_path)
        return factory(struct_or_path)

    def get_pipeline_factory(self, struct_or_path):
        try:
            # test for Path
            _ = struct_or_path.stat()
            return self._path_pipeline_factory
        except AttributeError:
            # could be an event (dict)
            try:
                _ = struct_or_path['Pipeline']
                # could be a well-formed event
                return self._struct_pipeline_factory
            except KeyError as ke:
                # was passed a dict-like missing the expected key
                raise ke
        except:
            raise ValueError('Unexpected input: provide path to YAML file or event dictionary')

    def _get_default_handler(self):
        return PipelineHandler(
            cluster_handler=ClusterHandler(),
            queues_handler=QueuesHandler(),
            inputs_handler=InputsHandler(),
            stages_handler=StagesHandler(
                stage_handler=StageHandler(
                    aggregate_handler=AggregateHandler(),
                    job_handler=JobHandler()
                )
            )
        )

    def _get_default_pipeline(self, pipeline_struct):

        pipeline_handler = self._get_default_handler()
        pipeline = pipeline_handler.handle(pipeline_struct)

        validator = PipelineValidator(pipeline)
        validator.validate()

        return pipeline

    def _struct_pipeline_factory(self, pipeline_struct):
        return self._get_default_pipeline(pipeline_struct)

    def _path_pipeline_factory(self, path):
        with path.open() as fh:
            pipeline_struct = yaml.safe_load(fh)

        return self._get_default_pipeline(pipeline_struct)


class Handler(ABC):

    @abstractmethod
    def handle(self, request):
        raise NotImplementedError


class PipelineValidator:

    def __init__(self, pipeline):
        self.pipeline = pipeline

    def validate(self):
        self.__validate_array_inputs()

    def __validate_array_inputs(self):
        for aggregate in self.pipeline.aggregates:
            if aggregate.type == 'array':
                input_ = self.pipeline.get_input(aggregate.name)
                if input_ is None:
                    raise ValueError('Must declare an Input for every array aggregate')


class PipelineHandler(Handler):

    def __init__(self, cluster_handler, queues_handler, inputs_handler, stages_handler):
        self.cluster_handler = cluster_handler
        self.queues_handler = queues_handler
        self.inputs_handler = inputs_handler
        self.stages_handler = stages_handler

    def handle(self, request):
        try:
            pipeline_struct = request['Pipeline']
        except KeyError:
            raise ValueError('The specification file must contain a Pipeline definition.')

        cluster = self.cluster_handler.handle(pipeline_struct)
        queues = self.queues_handler.handle(pipeline_struct)
        inputs = self.inputs_handler.handle(pipeline_struct)
        stages, aggregates = self.stages_handler.handle(pipeline_struct)

        return Pipeline(
            cluster=cluster,
            queues=queues,
            inputs=inputs,
            stages=stages,
            aggregates=aggregates
        )


class ClusterHandler(Handler):

    def handle_cpus(self, cluster_struct):
        try:
            cpus_struct = cluster_struct['cpus']
        except KeyError:
            raise ValueError('Cluster must contain a cpus definition.')

        required_keys = {'min', 'max', 'desired'}
        found_keys = set(cpus_struct.keys())

        if found_keys != required_keys:
            raise ValueError('cpus definition must have keys: {}'.format(required_keys))

        min_cpus = cpus_struct['min']
        max_cpus = cpus_struct['max']
        desired_cpus = cpus_struct['desired']

        return min_cpus, max_cpus, desired_cpus

    def handle_instances(self, cluster_struct):
        try:
            instances_struct = cluster_struct['instances']
        except KeyError:
            raise ValueError('Cluster must contain a instances definition.')

        try:
            instances = list(instances_struct)
        except TypeError:
            raise ValueError('Job definition Command must be a list')
        else:
            return instances

    def handle(self, request):
        try:
            cluster_struct = request['Cluster']
        except KeyError:
            raise ValueError('Pipeline must contain a Cluster definition.')

        min_cpus, max_cpus, desired_cpus = self.handle_cpus(cluster_struct)
        instance_list = self.handle_instances(cluster_struct)

        return Cluster(
            min_cpus=min_cpus,
            max_cpus=max_cpus,
            desired_cpus=desired_cpus,
            instance_types=instance_list
        )


class QueuesHandler(Handler):

    def handle(self, request):
        try:
            queues_struct = request['Queues']
        except KeyError:
            raise ValueError('Pipeline must contain a Queues definition.')

        queues = [Queue(queue_name) for queue_name in queues_struct if _valid_name(queue_name)]
        if len(queues_struct) != len(queues):
            raise ValueError('Queue names must be snake case')
        else:
            return queues


class InputsHandler(Handler):

    def _s3_uri(self, manifest_str):

        parts = urlparse(manifest_str)

        if parts.scheme.lower() != 's3':
            raise ValueError('Input manifests must be S3 URI\'s')

    def handle(self, request):
        try:
            input_structs = request['Inputs']
        except KeyError:
            # only required if there are array aggregates, which this handler doesn't know about.
            # there is a check for this condition in PipelineValidator (PipelineFactory._get_default_pipeline)
            return []

        inputs = [
            Input(input_struct['aggregate'], input_struct['manifest'])
            for input_struct in input_structs if _valid_name(input_struct['aggregate'])
            ]
        if len(input_structs) != len(inputs):
            raise ValueError('Input stage names must be snake case')
        else:
            return inputs


class StagesHandler(Handler):

    def __init__(self, stage_handler):
        self.stage_handler = stage_handler

    def handle(self, request):
        try:
            stage_structs = request['Stages']
        except KeyError:
            raise ValueError('A Pipeline must contain Stages')

        stages = []

        for stage_name, stage_struct in stage_structs.items():
            stage, aggregate = self.stage_handler.handle({stage_name: stage_struct})
            stages.append(stage)

        # Law of Demeter violation
        aggregates = list(self.stage_handler.aggregate_handler.name_to_agg.values())

        return stages, aggregates


class StageHandler(Handler):

    def __init__(self, aggregate_handler, job_handler):
        # ensures no duplicate stage names
        self.stage_names = []
        # the queue pointers we've seen: only used for validation
        self.queues = []
        # handlers for sub-elements
        self.aggregate_handler = aggregate_handler
        self.job_handler = job_handler

    def handle_name(self, stage_name):
        # ensure unique name for stage
        if stage_name in self.stage_names:
            return ValueError('Duplicate stage names: {}'.format(stage_name))
        else:
            self.stage_names.append(stage_name)

    def handle_queue(self, stage_dict):
        try:
            queue_name = stage_dict['queue']
        except KeyError:
            raise ValueError('Every Stage requires a Queue definition.')

        self.queues.append(
            Queue(queue_name)
        )

        return queue_name

    def handle_job(self, stage_dict):
        try:
            job_struct = stage_dict['job']
        except KeyError:
            raise ValueError('Every Stage requires a Job definition.')

        return self.job_handler.handle(job_struct)

    def handle_next_stages(self, stage_dict):
        try:
            next_struct = stage_dict['next']
        except KeyError:
            # no next stage defined, OK for now
            return []
        else:
            try:
                return list(next_struct)
            except TypeError:
                raise ValueError('Stage has Next defined, but it was not a list')

    def handle_aggregate(self, stage_dict, stage, stage_name):
        try:
            aggregate_struct = stage_dict['aggregate']
        except KeyError:
            raise ValueError('Every Stage requires an Aggregate definition.')

        return self.aggregate_handler.handle(
            {
                'struct': aggregate_struct,
                'stage': stage
            }
        )

    def handle(self, request):

        # expect dictionary with one item: stage_name -> stage_dict
        try:
            stage_names = request.keys()
        except AttributeError:
            raise ValueError('Expected a dictionary for stage.')
        else:
            try:
                stage_name = list(stage_names).pop()
            except IndexError:
                raise ValueError('Expected at least one stage name.')

            try:
                stage_dict = request[stage_name]
            except KeyError:
                raise ValueError('Could not retrieve Stage dictionary with name.')

        self.handle_name(stage_name)
        queue_name = self.handle_queue(stage_dict)
        job = self.handle_job(stage_dict)
        next_stages = self.handle_next_stages(stage_dict)

        stage = Stage(
            name=stage_name,
            job=job,
            queue_name=queue_name,
            next_list=next_stages
        )

        aggregate = self.handle_aggregate(stage_dict, stage, stage_name)

        return stage, aggregate


class AggregateHandler(Handler):

    valid_types = {'array', 'concurrent', 'simple'}

    def __init__(self):
        self.name_to_agg = dict()

    def handle_name(self, aggregate_struct):
        try:
            name = aggregate_struct['name']
        except KeyError:
            raise ValueError('Every Aggregate requires a name field.')
        else:
            if not _valid_name(name):
                raise ValueError('Aggregate names must be snake case')
            else:
                return name

    def handle_type(self, aggregate_struct):
        try:
            type_ = aggregate_struct['type']
        except KeyError:
            raise ValueError('Every Aggregate requires a type field.')
        else:
            if type_ not in AggregateHandler.valid_types:
                raise ValueError('Aggregate type must be one of: {}.'.format(AggregateHandler.valid_types))
            else:
                return type_

    def handle_branch(self, type_, aggregate_struct):
        if type_ != 'concurrent':
            return None

        try:
            branch_name = aggregate_struct['branch']
        except KeyError:
            raise ValueError('Every Concurrent Aggregate requires a Branch element.')
        else:
            if not _valid_name(branch_name):
                raise ValueError('Branch name must be snake case')
            else:
                return branch_name

    def find_or_create_aggregate(self, name, type_, stage):
        if name in self.name_to_agg:
            # pull agg out by name
            aggregate = self.name_to_agg[name]
            # append current stage
            aggregate.stages.append(stage)
        else:
            aggregate = Aggregate(
                name=name,
                type_=type_,
                stages=[stage]
            )
            self.name_to_agg[name] = aggregate
            return aggregate

    def handle(self, request):
        aggregate_struct = request['struct']
        stage = request['stage']

        name = self.handle_name(aggregate_struct)
        type_ = self.handle_type(aggregate_struct)

        aggregate = self.find_or_create_aggregate(name, type_, stage)

        return aggregate


class JobHandler(Handler):

    @staticmethod
    def handle_size(user_mem):
        try:
            user_mem_str = str(user_mem)
        except:
            raise ValueError('Error parsing memory value, must be a string or int, found: {}'.format(user_mem))
        else:
            size_user = bitmath.parse_string(user_mem_str)
            size_mb = size_user.to_MB()
            return int(size_mb)

    def handle(self, request):

        required_keys = {'cpus', 'mem', 'image', 'cmd'}
        found_keys = set(request.keys())

        if not required_keys.issubset(found_keys):
            print('Found keys: {}'.format(request.keys()))
            raise ValueError('Job definition must have keys: {}'.format(required_keys))

        try:
            cmd_list = list(request['cmd'])
        except TypeError:
            raise ValueError('Job definition Command must be a list')

        mem_size = JobHandler.handle_size(request['mem'])

        env = request['env'] if 'env' in found_keys else None

        return Job(
            cpus=request['cpus'],
            memory=mem_size,
            image=request['image'],
            cmd_list=cmd_list,
            env=env
        )


class Pipeline:

    def __init__(self, cluster, queues, inputs, stages, aggregates):
        self.cluster = cluster
        self.queues = queues
        self.inputs = inputs
        self.stages = stages
        self.aggregates = aggregates

    def _pop_match(self, matches):
        return matches.pop() if len(matches) == 1 else None

    def get_stage(self, stage_name):
        matches = [stage for stage in self.stages if stage.name == stage_name]
        print('Pipeline> matches for {}: {}'.format(stage_name, matches))
        return self._pop_match(matches)

    def get_aggregate_for_stage(self, stage_to_find):
        matches = [aggregate for aggregate in self.aggregates for stage in aggregate.stages if stage == stage_to_find]
        return self._pop_match(matches)

    def get_aggregate(self, aggregate_name):
        matches = [aggregate for aggregate in self.aggregates if aggregate.name == aggregate_name]
        return self._pop_match(matches)

    def get_queue(self, queue_name):
        matches = [queue for queue in self.queues if queue.name == queue_name]
        return self._pop_match(matches)

    def get_input(self, aggregate_name):
        matches = [input_ for input_ in self.inputs if input_.aggregate_name == aggregate_name]
        return self._pop_match(matches)


class Cluster:

    def __init__(self, min_cpus, max_cpus, desired_cpus, instance_types):
        self.min_cpus = min_cpus
        self.max_cpus = max_cpus
        self.desired_cpus = desired_cpus
        self.instance_types = instance_types


class Queue:

    def __init__(self, name):
        self.name = name


class Input:

    def __init__(self, aggregate_name, manifest):
        self.aggregate_name = aggregate_name
        self.manifest = manifest


class Aggregate:

    def __init__(self, name, type_, stages):
        self.name = name
        self.type = type_
        self.stages = stages if stages is not None else []

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return '.'.join((self.name, self.type))


class Stage:

    def __init__(self, name, job, queue_name, next_list):
        self.name = name
        self.job = job
        self.queue_name = queue_name
        self.next_list = next_list

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return (
            isinstance(other, Stage) and
            self.name == other.name
        )

    def __hash__(self):
        return hash(self.name)


class Job:

    def __init__(self, cpus, memory, image, cmd_list, env=None):
        self.cpus = cpus
        self.memory = memory
        self.image = image
        self.cmd_list = cmd_list
        self.env = env