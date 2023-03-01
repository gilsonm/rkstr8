import boto3
import yaml
import copy
from uuid import uuid4
from pathlib import Path
from pprint import pprint
from rkstr8.cloud import Command
from rkstr8.cloud.cloudformation import TemplateProcessor
from rkstr8.conf import Config

batch_client = boto3.client('batch')

#
# POLLING OUTCOME TOKENS
#
POLLING_SUCCESS_TOKEN = 'SUCCESS'
POLLING_FAIL_TOKEN = 'FAIL'
POLLING_IN_PROGRESS_TOKEN = 'IN_PROGRESS'

#
# BATCH JOB STATUS TOKENS
#
BATCH_FAIL_STATUS = 'FAILED'
BATCH_SUCCESS_STATUS = 'SUCCEEDED'


class BatchJobDefDiscovery:

    def __init__(self, config: Config):
        self.batch_config = config.batch_config

    def discover_stages(self, pipeline):
        stages = pipeline.stages
        print(stages)
        return stages




class ResourceRenderer:

    def __init__(self):
        self.template_base = None
        self.uid = str(uuid4())[:4]

    def logical_id_from_resource(self, resource):
        return list(resource).pop()

    def _to_camel(self, snake_string):
        return ''.join([p.capitalize() for p in snake_string.split('_')])

    def logical_resource_id(self, name, resource_type, from_snake=False):
        return '{}{}{}'.format(
            self._to_camel(name) if from_snake else name,
            resource_type,
            self.uid
        )

    def set_depends_on(self, template, dependencies):
        template['DependsOn'] = dependencies

    def _load_template_base(self, template_string):
        template_path = Path(template_string)

        with template_path.open('r') as template_fh:
            self.template_base = yaml.safe_load(template_fh)

    def get_template(self, template_string):
        if self.template_base is None:
            self._load_template_base(template_string)
        return copy.deepcopy(self.template_base)


class BatchJobDefRendering(ResourceRenderer):

    def __init__(self, config: Config):
        super().__init__()
        self.batch_config = config.batch_config

    def set_container_property(self, template, property, value):
        template['Properties']['ContainerProperties'][property] = value

    def set_name(self, template, name):
        template['Properties']['JobDefinitionName'] = self.logical_resource_id(
            name=name,
            resource_type='JobDef',
            from_snake=True
        )

    def render(self, stage, resolver):
        job_template_file = resolver(self.batch_config.job_template_file)
        template = self.get_template(job_template_file)

        job = stage.job

        container_properties = {
            'Image': job.image,
            'Memory': job.memory,
            'Command': job.cmd_list,
            'Vcpus': job.cpus,
            'JobRoleArn': {'Ref': self.batch_config.job_role_lri}
        }

        for property, value in container_properties.items():
            self.set_container_property(template, property, value)

        self.set_depends_on(template, [self.batch_config.job_role_lri])
        self.set_name(template, stage.name)

        logical_id = self.logical_resource_id(stage.name, 'JobDef', from_snake=True)

        return {
            logical_id: template
        }


class BatchComputeEnvironmentRendering(ResourceRenderer):

    def __init__(self, config: Config):
        super().__init__()
        self.batch_config = config.batch_config

    def set_compute_resources_property(self, template, property, value):
        template['Properties']['ComputeResources'][property] = value

    def render(self, cluster, resolver):
        cluster_template_file = resolver(self.batch_config.cluster_template_file)
        template = self.get_template(cluster_template_file)

        compute_properties = {
            'MinvCpus': cluster.min_cpus,
            'MaxvCpus': cluster.max_cpus,
            'DesiredvCpus': cluster.desired_cpus,
            'InstanceTypes': cluster.instance_types,
            'Ec2KeyPair': self.batch_config.ssh_key
        }

        # conditionally set the cluster AMI if user config specifies it.
        # if not set, Batch selects default.
        if self.batch_config.cluster_ami:
            compute_properties['ImageId'] = self.batch_config.cluster_ami

        for property, value in compute_properties.items():
            self.set_compute_resources_property(template, property, value)

        resource_id = self.logical_resource_id('GeneralPurpose','Cluster')

        return {
            resource_id: template
        }


class BatchQueueRendering(ResourceRenderer):

    def __init__(self, config: Config):
        super().__init__()
        self.batch_config = config.batch_config

    def set_compute_environment_order(self, template, compute_environment_resource_id, order):
        template['Properties']['ComputeEnvironmentOrder'] = [
            {
                'ComputeEnvironment': {'Ref': compute_environment_resource_id},
                'Order': order
            }
        ]

    def set_job_queue_name(self, template, queue_name):
        template['Properties']['JobQueueName'] = {
            'Fn::Join': ['-', [queue_name, {'Ref': 'StackUID'}] ]
        }

    def render(self, queue, compute_environment, resolver):
        queue_template_file = resolver(self.batch_config.queue_template_file)
        template = self.get_template(queue_template_file)

        compute_environment_resource_id = list(compute_environment).pop()

        self.set_compute_environment_order(template, compute_environment_resource_id, 1)
        self.set_job_queue_name(template, queue.name)
        self.set_depends_on(template, ['BatchServiceRole', compute_environment_resource_id])

        logical_id = self.logical_resource_id(queue.name, 'Queue', from_snake=True)

        return {
            logical_id: template
        }


class BatchTemplateRendering(ResourceRenderer):

    def __init__(self, config: Config):
        super().__init__()
        self.batch_config = config.batch_config

    def separate_resource_and_ids(self, resources):
        # [ {id_0: resource_0}, {id_1: resource_1}... ]
        resource_ids = [self.logical_id_from_resource(r) for r in resources]
        resource_values = [list(r.values()).pop() for r in resources]

        return resource_ids, resource_values

    def render(self, job_defs, cluster, queues, resolver):

        cluster_resource_id = self.logical_id_from_resource(cluster)
        queue_resource_ids, queue_resources = self.separate_resource_and_ids(queues)
        job_def_resource_ids, job_def_resources = self.separate_resource_and_ids(job_defs)

        base_template_file = resolver(self.batch_config.base_template_file)
        batch_template_rendering_pipeline = TemplateProcessor([base_template_file]) \
            .from_yaml(as_path=True) \
            .add_resource(cluster_resource_id, cluster[cluster_resource_id])\
            .add_resources(queue_resource_ids, queue_resources) \
            .add_resources(job_def_resource_ids, job_def_resources)\
            .to_yaml()

        batch_template_rendered = list(batch_template_rendering_pipeline)[0]
        rendered_template_file = resolver(self.batch_config.rendered_template_file)
        template_output_file = Path(rendered_template_file)

        with template_output_file.open(mode='w') as template_out_fh:
            template_out_fh.write(batch_template_rendered )


class BatchJobListStatusPoller:
    '''
    Polls the status of a list of Batch Jobs, returning a "Polling Outcome" token.
    '''

    def __init__(self, job_ids):
        self.job_ids = job_ids if job_ids is not None else []

    @staticmethod
    def split_list(l, n):
        '''Yield successive n-sized chunks from l.'''
        for i in range(0, len(l), n):
            yield l[i:i + n]

    @staticmethod
    def statuses_for_jobs(job_descriptions):
        '''
        For each job, one of:
            'SUBMITTED'|'PENDING'|'RUNNABLE'|'STARTING'|'RUNNING'|'SUCCEEDED'|'FAILED'
        '''
        return [job['status'] for job in job_descriptions['jobs']]

    def polling_outcome(self):
        all_job_stats = self.job_stats()
        polling_outcome = self.polling_outcome_token(all_job_stats)
        print('outcome: {}'.format(polling_outcome))

        return polling_outcome

    def job_stats(self):
        all_job_stats = []
        for sublist_of_jobs in self.split_list(self.job_ids, 100):
            partial_job_descriptions = batch_client.describe_jobs(jobs=sublist_of_jobs)
            partial_job_stats = self.statuses_for_jobs(partial_job_descriptions)
            all_job_stats += (partial_job_stats)
        return all_job_stats

    def polling_outcome_token(self, job_stats):
        '''
        Takes in sequence over this set, call it JobStat:
            'SUBMITTED'|'PENDING'|'RUNNABLE'|'STARTING'|'RUNNING'|'SUCCEEDED'|'FAILED'

        And returns one term from this set, call it TaskStat:
            'SUCCESS'|'FAIL'|'IN_PROGRESS'

        I.e. This method implements a single valued function of many variables:
        F: JobStat^k -> TaskStat
        Where JobStat^k is the Cartesian product of Jobstat with itself, k times.
        k is the number of jobs we're polling for completion.
        '''
        print(job_stats)
        current_states = set(job_stats)

        # if all states are succeeded, outcome is SUCCESS_TOKEN
        if current_states == {BATCH_SUCCESS_STATUS}:
            return POLLING_SUCCESS_TOKEN

        # if there is one or more FAILED jobs, outcome is FAIL_TOKEN
        if BATCH_FAIL_STATUS in current_states:
            return POLLING_FAIL_TOKEN

        return POLLING_IN_PROGRESS_TOKEN

    def polling_outcome_for_queue(self, queue_id):

        for status in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING', 'SUCCEEDED', 'FAILED']:
            # get all jobIds in queue of each status
            next_token = ''
            while next_token != 'null':
                sub_response = batch_client.list_jobs(
                    jobQueue=queue_id,
                    jobStatus=status,
                    maxResults=100,
                    nextToken=next_token
                )
                pprint(sub_response)
                if len(sub_response['jobSummaryList']) > 100:
                    # next_token only present in resp
                    # if jobSummaryList has more than
                    # maxResults items in it
                    next_token = sub_response['nextToken']
                else:
                    # exit while loop -  no need to paginate
                    next_token = 'null'

                for job in sub_response['jobSummaryList']:
                    # Only add new job ids since a job could
                    # have switched statuses since last poll
                    if job['jobId'] not in self.job_ids:
                        self.job_ids.append(job['jobId'])
                print("NEXT_TOKEN: {}".format(next_token))

        print(self.job_ids)

        return self.polling_outcome()

