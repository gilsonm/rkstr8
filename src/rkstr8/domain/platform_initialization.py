from rkstr8.cloud.lambda_ import deploy, LambdaDeployment, LambdaDiscovery, LambdaTemplateRendering, HandlerLocal, Deployment, TemplateExport
from rkstr8.cloud.batch import BatchJobDefDiscovery, BatchJobDefRendering, BatchComputeEnvironmentRendering, BatchQueueRendering, BatchTemplateRendering
from rkstr8.domain.pipeline import *
from rkstr8.domain.actions import Rkstr8PackageResourceCopyAction, PipelineUnmarshallingAction, Rkstr8PackagingAction, LambdaDiscoveryAction, LambdaDeploymentAction, BatchJobDefDiscoveryAction, BatchJobDefRenderingAction, BatchClusterRenderingAction, BatchQueueRenderingAction, BatchTemplateRenderingAction, BestEffortExecutionPlanAction, StateMachineRenderingAction, LambdaTemplateAction, CloudFormationTemplateDeploymentAction, CloudFormationStackLaunchingAction, CloudFormationStackWaitingAction, StageGraphBuildingAction, AggregateGraphBuildingAction, StepFunctionsEventBuildingAction, StepFunctionsEventUploadingAction, StepFunctionsExecutionAction, AssetStagingAction, DynamoDbInitialGraphColoring, DockerImageBuildingAction
from rkstr8.domain.workers import *
from rkstr8.domain.ioc import Executor, State
from rkstr8.conf import Config

from queue import Queue
import logging

# for plotting
import networkx as nx
import subprocess
import shutil

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s [%(levelname)s]: %(message)s')

state_colors = {
    State.PENDING: 'white',
    State.READY: 'grey',
    State.ENQUEUED: 'yellow',
    State.RUNNING: 'orange',
    State.SUCCESS: 'green',
    State.FAIL: 'red',
}


class Platform:

    def __init__(self, config: Config):
        self.config: Config = config

    def initialize(self):
        self.actions, self.q = self.create_and_wire_actions()

    def run(self):
        assert self.actions and self.q
        dry_run = self.config.launch_config.dry_run
        try:
            executor = Executor(self.q, self.actions)
            executor.execute(dry_run=dry_run, parallel=True)
        finally:
            self.plot_execution(self.actions,'actions.dot')
            self.cleanup()

    def plot_execution(self, actions, filename):
        name = lambda x: x.__class__.__name__
        deps = nx.DiGraph()
        for act in actions:
            for prop,dep in act.dependencies.items():
                deps.add_edge(name(dep), name(act), label=prop)
        styles = {name(act): 'filled' for act in actions}
        colors = {name(act): state_colors[act.state] for act in actions}
        nx.set_node_attributes(deps, colors, 'fillcolor')
        nx.set_node_attributes(deps, styles, 'style')
        nx.drawing.nx_pydot.write_dot(deps, filename)
        if shutil.which('dot'):
            subprocess.check_call(['dot', '-Tpng', filename, '-o', f'{filename}.png'])

    def create_and_wire_actions(self):
        q = Queue()
        c = self.config

        # By convention the shared objects produced and consumed by actions should be handled in a write-once, then
        # read only fashion.  This means that the first action to produce a shared object should be the only action to
        # create or mutate the object.
        #
        # Future work will enforce only immutable collections can be exchanged between actions. Many/Most are already
        # immutable (nested namedtuples of strings).

        pkg_resource_copy = Rkstr8PackageResourceCopyAction(
            queue=q,
            config=c
        )

        pipeline_parsing = PipelineUnmarshallingAction(
            queue=q,
            worker=UnmarshallYamlPipeline(config=c),
            resolver=pkg_resource_copy
        )

        lambda_discovery = LambdaDiscoveryAction(
            queue=q,
            worker=LambdaDiscovery(config=c),
            resolver=pkg_resource_copy
        )

        wheel_building = Rkstr8PackagingAction(
            queue=q,
            worker=Rkstr8WheelManager(config=c),
            config=c # conditionally build a wheel, could be pushed into worker
        )

        lambda_deployment = LambdaDeploymentAction(
            queue=q,
            worker=LambdaDeployment(config=c),
            handlers=lambda_discovery,
            wheel=wheel_building
        )

        batch_job_discovery = BatchJobDefDiscoveryAction(
            queue=q,
            worker=BatchJobDefDiscovery(config=c),
            pipeline=pipeline_parsing
        )

        batch_job_rendering = BatchJobDefRenderingAction(
            queue=q,
            worker=BatchJobDefRendering(config=c),
            stages=batch_job_discovery,
            resolver=pkg_resource_copy
        )

        batch_cluster_rendering = BatchClusterRenderingAction(
            queue=q,
            worker=BatchComputeEnvironmentRendering(config=c),
            pipeline=pipeline_parsing,
            resolver=pkg_resource_copy
        )

        batch_queue_rendering = BatchQueueRenderingAction(
            queue=q,
            worker=BatchQueueRendering(config=c),
            compute_environment=batch_cluster_rendering,
            resolver=pkg_resource_copy,
            pipeline=pipeline_parsing
        )

        batch_template_rendering = BatchTemplateRenderingAction(
            queue=q,
            worker=BatchTemplateRendering(config=c),
            job_defs=batch_job_rendering,
            compute_environment=batch_cluster_rendering,
            queue_resources=batch_queue_rendering,
            resolver=pkg_resource_copy
        )

        best_effort_planner = BestEffortExecutionPlanAction(
            queue=q,
            worker=BestEffortStateMachineBuilder(config=c)
        )

        state_machine_rendering = StateMachineRenderingAction(
            queue=q,
            worker=StateMachineRenderer(config=c),
            machine=best_effort_planner,
            resolver=pkg_resource_copy
        )

        lambda_templating = LambdaTemplateAction(
            queue=q,
            worker=LambdaTemplateRendering(config=c),
            handler_deployments=lambda_deployment,
            resolver = pkg_resource_copy
        )

        cloudformation_deployment = CloudFormationTemplateDeploymentAction(
            queue=q,
            worker=CloudFormationTemplateDeployer(config=c),
            resolver=pkg_resource_copy,
            lambda_templating=lambda_templating,
            state_machine_rendering=state_machine_rendering,
            batch_template_rendering=batch_template_rendering
        )

        cloudformation_stack_launching = CloudFormationStackLaunchingAction(
            queue=q,
            worker=CloudFormationStackLauncher(config=c),
            stack_template_url=cloudformation_deployment,
            stack_template_params=cloudformation_deployment,
            lambda_templating=lambda_templating,
            state_machine_rendering=state_machine_rendering,
            batch_template_rendering=batch_template_rendering
        )

        cloudformation_launch_waiting = CloudFormationStackWaitingAction(
            queue=q,
            worker=CloudFormationStackWaiter(config=c),
            cloudformation_stack_launching=cloudformation_stack_launching
        )

        graph_builder = PipelineGraphBuilder()

        stage_graph_builder = StageGraphBuildingAction(
            queue=q,
            worker=graph_builder,
            pipeline=pipeline_parsing
        )

        aggregate_graph_builder = AggregateGraphBuildingAction(
            queue=q,
            worker=graph_builder,
            pipeline=pipeline_parsing,
            stage_graph=stage_graph_builder
        )

        step_functions_event_building = StepFunctionsEventBuildingAction(
            queue=q,
            worker=StepFunctionsEventBuilder(config=c),
            cloudformation_launch_waiting=cloudformation_launch_waiting
        )

        step_functions_event_uploading = StepFunctionsEventUploadingAction(
            queue=q,
            worker=StepFunctionsEventUploader(config=c),
            event=step_functions_event_building
        )

        asset_staging = AssetStagingAction(
            queue=q,
            worker=AssetStager(config=c)
        )

        # in the future we can factor out dynamodb and simply use
        # s3 for our database, as the key design for dynamo was
        # developed to support key-value stores. However we had
        # additional data more appropriate for a document store
        # but those requirements we factored out.
        graph_initial_coloring = DynamoDbInitialGraphColoring(
            queue=q,
            worker=PopulateInitialColoring(config=c),
            stage_graph=stage_graph_builder,
            aggregate_graph=aggregate_graph_builder,
            cloudformation_launch_waiting=cloudformation_launch_waiting
        )

        # Also pushes
        docker_image_building = DockerImageBuildingAction(
            queue=q,
            worker=DockerImageManager(config=c),
            pipeline_config=c.pipeline_config
        )

        # in the future we can factor out step functions and use
        # the simple cron-syntax event source to trigger the execution
        # of the lambda scheduler function, which is all the stepfunctions
        # executor is currently doing. Previous iterations encoded pipelines
        # as step functions and so we still have remnants.
        step_functions_executions = StepFunctionsExecutionAction(
            queue=q,
            worker=StepFunctionsExecutor(config=c),
            event_s3_uri=step_functions_event_uploading,
            cloudformation_launch_waiting=cloudformation_launch_waiting,
            asset_staging=asset_staging,
            dynamodb_initial_graph_coloring=graph_initial_coloring,
            docker_image_building=docker_image_building
        )

        # any iterable type will suffice, order does not matter.
        # execution order is determined by the action dependency graph
        actions = {
            pkg_resource_copy,
            pipeline_parsing,
            wheel_building,
            lambda_discovery,
            lambda_deployment,
            batch_job_discovery,
            batch_job_rendering,
            batch_cluster_rendering,
            batch_queue_rendering,
            batch_template_rendering,
            best_effort_planner,
            state_machine_rendering,
            lambda_templating,
            cloudformation_deployment,
            cloudformation_stack_launching,
            cloudformation_launch_waiting,
            stage_graph_builder,
            aggregate_graph_builder,
            step_functions_event_building,
            step_functions_event_uploading,
            step_functions_executions,
            asset_staging,
            graph_initial_coloring,
            docker_image_building
        }

        return actions, q

    def cleanup(self):
        for action in self.actions:
            action.cleanup()

