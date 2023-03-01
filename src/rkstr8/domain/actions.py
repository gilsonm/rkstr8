from rkstr8.domain.ioc import Action
from rkstr8.domain.workers import Rkstr8PackageResourceCopy, Rkstr8WheelManager, BestEffortStateMachineBuilder, StateMachineRenderer, CloudFormationTemplateDeployer, CloudFormationStackLauncher, CloudFormationStackWaiter, StepFunctionsEventBuilder, StepFunctionsExecutor, StepFunctionsEventUploader, AssetStager, PopulateInitialColoring, DockerImageManager
from rkstr8.domain.pipeline import UnmarshallYamlPipeline, PipelineGraphBuilder
from rkstr8.cloud.lambda_ import LambdaDiscovery, LambdaDeployment, LambdaTemplateRendering
from rkstr8.cloud.batch import BatchJobDefDiscovery, BatchJobDefRendering, BatchComputeEnvironmentRendering, BatchQueueRendering, BatchTemplateRendering
from rkstr8.conf import Config, PipelineConfig

from typing import Union
from pathlib import Path
import yaml
import queue
import os


class Rkstr8PackageResourceCopyAction(Action):

    def __init__(self, queue: queue.Queue, config: Config):
        produces = ['resolver']
        dependencies = {}
        self.config = config.launch_config
        super().__init__(queue, produces, dependencies)

    def work(self, dry_run=False):
        self.pkg_resource_tempdir = self.config.pkg_data_dir
        return [self.pkg_local_path]

    def pkg_local_path(self, pkg_resource: Union[str, Path]) -> str:
        ''' Thread safe. Called by multiple workers, possibly concurrently. '''
        if not self.pkg_resource_tempdir:
            raise ValueError('Package resource tempdir not yet set in initialization')
        local_path = os.path.join(self.pkg_resource_tempdir, pkg_resource)
        return local_path


class PipelineUnmarshallingAction(Action):

    def __init__(self, queue: queue.Queue, worker: UnmarshallYamlPipeline,
                resolver: Rkstr8PackageResourceCopyAction):
        produces = ['pipeline']
        dependencies = { 'resolver': resolver }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, resolver, dry_run=False):
        pipeline = self.worker.unmarshall_pipeline(resolver)
        return [pipeline]


class LambdaDiscoveryAction(Action):

    def __init__(self, queue: queue.Queue, worker: LambdaDiscovery,
                 resolver: Rkstr8PackageResourceCopyAction):
        produces = ['handler_locals']
        dependencies = { 'resolver': resolver }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, pkg_local_path, dry_run=False):
        handler_locals = self.worker.handler_locals(resolver=pkg_local_path)
        return [handler_locals]


class Rkstr8PackagingAction(Action):

    def __init__(self, queue: queue.Queue, worker: Rkstr8WheelManager, config: Config):
        produces = ['rkstr8_wheel']
        dependencies = {}
        self.worker = worker
        self.launch_config = config.launch_config
        self.pipeline_config = config.pipeline_config
        super().__init__(queue, produces, dependencies)

    def work(self, dry_run=False):
        do_build = self.launch_config.do_pkg_rkstr8 and not dry_run
        rkstr8_wheel = self.worker.get_wheel(build=do_build)
        if not os.path.exists(rkstr8_wheel):
            raise ValueError(f'Path {rkstr8_wheel} does not exist')
        return [rkstr8_wheel]


class LambdaDeploymentAction(Action):

    def __init__(self, queue: queue.Queue, worker: LambdaDeployment,
                 handlers: LambdaDiscoveryAction,
                 wheel: Rkstr8PackagingAction):
        produces = ['handler_deployments']
        dependencies = {
            'handler_locals': handlers,
            'rkstr8_wheel': wheel,
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, handler_locals, rkstr8_wheel, dry_run=False):
        handler_deployments = [] if dry_run else self.worker.stage_deployments(handler_locals, rkstr8_wheel)
        return [handler_deployments]


class BatchJobDefDiscoveryAction(Action):

    def __init__(self, queue: queue.Queue, worker: BatchJobDefDiscovery,
                 pipeline: PipelineUnmarshallingAction):
        produces = ['stages']
        dependencies = {
            'pipeline': pipeline,
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, pipeline, dry_run=False):
        stages = self.worker.discover_stages(pipeline)
        return [stages]


class BatchJobDefRenderingAction(Action):

    def __init__(self, queue: queue.Queue, worker: BatchJobDefRendering,
                 stages: BatchJobDefDiscoveryAction,
                 resolver: Rkstr8PackageResourceCopyAction):
        produces = ['job_defs']
        dependencies = {
            'stages': stages,
            'resolver': resolver
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, stages, resolver, dry_run=False):
        job_defs = [self.worker.render(stage, resolver) for stage in stages]
        for job_def in job_defs:
            print(yaml.dump(job_def))
        return [job_defs]


class BatchClusterRenderingAction(Action):

    def __init__(self, queue: queue.Queue, worker: BatchComputeEnvironmentRendering,
                 pipeline: PipelineUnmarshallingAction,
                 resolver: Rkstr8PackageResourceCopyAction):
        produces = ['compute_environment']
        dependencies = {
            'pipeline': pipeline,
            'resolver': resolver
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, pipeline, resolver, dry_run=False):
        cluster = pipeline.cluster
        compute_environment = self.worker.render(cluster, resolver)
        print(yaml.dump(compute_environment))
        return [compute_environment]


class BatchQueueRenderingAction(Action):

    def __init__(self, queue: queue.Queue, worker: BatchQueueRendering,
                 compute_environment: BatchClusterRenderingAction,
                 resolver: Rkstr8PackageResourceCopyAction,
                 pipeline: PipelineUnmarshallingAction):
        produces = ['queue_resources']
        dependencies = {
            'compute_environment': compute_environment,
            'resolver': resolver,
            'pipeline': pipeline
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, compute_environment, resolver, pipeline, dry_run=False):
        queue_resources = []
        for queue in pipeline.queues:
            queue_resource = self.worker.render(queue, compute_environment, resolver)
            print(yaml.dump(queue_resource))
            queue_resources.append(queue_resource)
        return [queue_resources]


class BatchTemplateRenderingAction(Action):

    def __init__(self, queue: queue.Queue, worker: BatchTemplateRendering,
                 job_defs: BatchJobDefRenderingAction,
                 compute_environment: BatchClusterRenderingAction,
                 queue_resources: BatchQueueRenderingAction,
                 resolver: Rkstr8PackageResourceCopyAction):
        produces = []
        dependencies = {
            'job_defs': job_defs,
            'compute_environment': compute_environment,
            'queue_resources': queue_resources,
            'resolver': resolver,
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, job_defs, compute_environment, queue_resources, resolver, dry_run=False):
        self.worker.render(job_defs, compute_environment, queue_resources, resolver)


class BestEffortExecutionPlanAction(Action):

    def __init__(self, queue: queue.Queue, worker: BestEffortStateMachineBuilder):
        produces = ['machine']
        dependencies = {}
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, dry_run=False):
        machine = self.worker.plan()
        return [machine]


class StateMachineRenderingAction(Action):

    def __init__(self, queue: queue.Queue, worker: StateMachineRenderer,
                 machine: BestEffortExecutionPlanAction,
                 resolver: Rkstr8PackageResourceCopyAction):
        produces = []
        dependencies = {
            'machine': machine,
            'resolver': resolver
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, machine, resolver, dry_run=False):
        self.worker.render(machine, resolver)


class LambdaTemplateAction(Action):

    def __init__(self, queue: queue.Queue, worker: LambdaTemplateRendering,
                 handler_deployments: LambdaDeploymentAction,
                 resolver: Rkstr8PackageResourceCopyAction):
        produces = ['template_exports']
        dependencies = {
            'handler_deployments': handler_deployments,
            'resolver': resolver
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, handler_deployments, resolver, dry_run=False):
        self.worker.render_and_write_template(handler_deployments, resolver)
        template_exports = self.worker.get_template_exports()
        return [template_exports]


class CloudFormationTemplateDeploymentAction(Action):

    def __init__(self, queue: queue.Queue, worker: CloudFormationTemplateDeployer,
                 resolver: Rkstr8PackageResourceCopyAction,
                 lambda_templating: LambdaTemplateAction,
                 state_machine_rendering: StateMachineRenderingAction,
                 batch_template_rendering: BatchTemplateRenderingAction
                 ):
        produces = ['stack_template_url', 'stack_template_params']
        dependencies = {
            'resolver': resolver,
            '$lambda_templating': lambda_templating,
            '$state_machine_rendering': state_machine_rendering,
            '$batch_template_rendering': batch_template_rendering
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, resolver, dry_run=False):
        stack_template_url, stack_template_params = self.worker.deploy(resolver)
        return [stack_template_url, stack_template_params]


class CloudFormationStackLaunchingAction(Action):

    def __init__(self, queue: queue.Queue, worker: CloudFormationTemplateDeployer,
                 stack_template_url: CloudFormationTemplateDeploymentAction,
                 stack_template_params: CloudFormationTemplateDeploymentAction,
                 lambda_templating: LambdaTemplateAction,
                 state_machine_rendering: StateMachineRenderingAction,
                 batch_template_rendering: BatchTemplateRenderingAction):
        produces = []
        dependencies = {
            'stack_template_url': stack_template_url,
            'stack_template_params': stack_template_params,
            '$lambda_templating': lambda_templating,
            '$state_machine_rendering': state_machine_rendering,
            '$batch_template_rendering': batch_template_rendering,
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, stack_template_url, stack_template_params, dry_run=False):
        if not dry_run:
            self.worker.launch(
                platform_template_s3_url=stack_template_url,
                params_to_template_s3_urls=stack_template_params
            )


class CloudFormationStackWaitingAction(Action):

    def __init__(self, queue: queue.Queue, worker: CloudFormationTemplateDeployer,
                 cloudformation_stack_launching: CloudFormationStackLaunchingAction):
        produces = []
        dependencies = {
            '$cloudformation_stack_launching': cloudformation_stack_launching,
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, dry_run=False):
        if not dry_run:
            self.worker.wait()


class StageGraphBuildingAction(Action):

    def __init__(self, queue: queue.Queue, worker: PipelineGraphBuilder,
                 pipeline: PipelineUnmarshallingAction):
        produces = ['stage_graph']
        dependencies = { 'pipeline': pipeline }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, pipeline, dry_run=False):
        stage_graph = self.worker.stage_graph(pipeline)
        return [stage_graph]


class AggregateGraphBuildingAction(Action):

    def __init__(self, queue: queue.Queue, worker: PipelineGraphBuilder,
                 pipeline: PipelineUnmarshallingAction,
                 stage_graph: StageGraphBuildingAction):
        produces = ['aggregate_graph']
        dependencies = {
            'pipeline': pipeline,
            'stage_graph': stage_graph
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, pipeline, stage_graph, dry_run=False):
        aggregate_graph = self.worker.aggregate_graph(pipeline, stage_graph)
        return [aggregate_graph]


class StepFunctionsEventBuildingAction(Action):

    def __init__(self, queue: queue.Queue, worker: StepFunctionsEventBuilder,
                 cloudformation_launch_waiting=CloudFormationStackWaitingAction
                 ):
        produces = ['event']
        dependencies = {
            '$cloudformation_launch_waiting': cloudformation_launch_waiting
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, dry_run=False):
        if not dry_run:
            event, event_filename = self.worker.build_event()
            return [event]
        else:
            return ['mock_event']


class StepFunctionsEventUploadingAction(Action):

    def __init__(self, queue: queue.Queue, worker: StepFunctionsEventUploader,
                 event: StepFunctionsEventBuildingAction):
        produces = ['event_s3_uri']
        dependencies = {
            'event': event,
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, event, dry_run=False):
        if not dry_run:
            event_s3_uri, event_filename = self.worker.upload(event)
            return [event_s3_uri]
        else:
            return ['mock event']


class AssetStagingAction(Action):

    def __init__(self, queue: queue.Queue, worker: AssetStager):
        produces = []
        dependencies = {}
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, dry_run=False):
        if not dry_run:
            self.worker.stage()


class DynamoDbInitialGraphColoring(Action):

    def __init__(self, queue: queue.Queue, worker: PopulateInitialColoring,
                 stage_graph: StageGraphBuildingAction,
                 aggregate_graph: AggregateGraphBuildingAction,
                 cloudformation_launch_waiting: CloudFormationStackWaitingAction):
        produces = []
        dependencies = {
            'stage_graph': stage_graph,
            'aggregate_graph': aggregate_graph,
            '$cloudformation_launch_waiting': cloudformation_launch_waiting
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, stage_graph, aggregate_graph, dry_run=False):
        if not dry_run:
            self.worker.populate(stage_graph, aggregate_graph)


class DockerImageBuildingAction(Action):

    def __init__(self, queue: queue.Queue, worker: DockerImageManager, pipeline_config: PipelineConfig):
        produces = []
        dependencies = {}
        self.worker = worker
        self.config = pipeline_config
        super().__init__(queue, produces, dependencies)

    def work(self, dry_run=False):
        if not dry_run:
            if self.config.docker_build or self.config.docker_push:
                discover_ok = self.worker.discover()
                if self.config.docker_build:
                    self.worker.build()
                if self.config.docker_push:
                    self.worker.push()


class StepFunctionsExecutionAction(Action):

    def __init__(self, queue: queue.Queue, worker: StepFunctionsExecutor,
                 event_s3_uri: StepFunctionsEventUploadingAction,
                 cloudformation_launch_waiting: CloudFormationStackWaitingAction,
                 asset_staging: AssetStagingAction,
                 dynamodb_initial_graph_coloring: DynamoDbInitialGraphColoring,
                 docker_image_building: DockerImageBuildingAction):
        produces = []
        dependencies = {
            'event_s3_uri': event_s3_uri,
            '$cloudformation_launch_waiting': cloudformation_launch_waiting,
            '$asset_staging': asset_staging,
            '$dynamodb_initial_graph_coloring': dynamodb_initial_graph_coloring,
            '$docker_image_building': docker_image_building
        }
        self.worker = worker
        super().__init__(queue, produces, dependencies)

    def work(self, event_s3_uri, dry_run=False):
        if not dry_run:
            self.worker.start_execution(event_s3_uri)

