from rkstr8.cloud import BotoClientFactory, BotoResourceFactory, Service, StackLaunchException
from rkstr8.cloud.s3 import S3Upload

from botocore.client import ClientError
import yaml
import logging
import io
import time

logger = logging.getLogger(__name__)


class TemplateProcessor:
    '''
    Builder pattern implementation of CloudFormation YAML Template rendering pipeline.
    Reads/Writes YAML. Functions to add resource definitions to the Resources block.

    Usage:
        1. Build a pipeline using the builder pattern. E.g.

        templates = ['templates/rkstr8.stack.yaml']

        rendered_templates = TemplateProcessor(templates)\
            .from_yaml()\
            .add_resource('MyStateMachine', state_machine_resource)\
            .add_resources(job_def_names, job_defs)\
            .to_yaml()

        2. Run pipeline by casting the instance to a list, which will invoke the __iter__ function, thus applying the
           pipeline stages to the collection elements. Results of the pipeline are left in the list.

        final_template = list(rendered_templates)[0]
    '''

    def __init__(self, collection):
        '''
        If providing a path as collection, wrap the string in a sequence type, e.g. list
        '''
        self.collection = collection
        self.pipeline = []

    def __iter__(self):
        for item in self.collection:
            for stage in self.pipeline:
                item = stage(item)
            yield item

    def from_yaml(self, as_path=True):
        def _from_yaml(path_or_string):
            stream = open(path_or_string, 'r') if as_path else io.StringIO(path_or_string)
            return yaml.safe_load(stream)
        self.pipeline.append(_from_yaml)
        return self

    def add_resource(self, name, definition):
        def _add_element(yaml_data):
            yaml_data['Resources'][name] = definition
            return yaml_data
        self.pipeline.append(_add_element)
        return self

    def add_resources(self, names, definitions):
        if len(names) != len(definitions):
            raise Exception('Resource names and definitions not of same length')
        def _add_elements(yaml_data):
            for name, definition in zip(names, definitions):
                yaml_data['Resources'][name] = definition
            return yaml_data
        self.pipeline.append(_add_elements)
        return self

    def add_exports(self, names, definitions):
        if len(names) != len(definitions):
            raise Exception('Resource names and definitions not of same length')
        def _add_exports(yaml_data):
            if 'Outputs' not in yaml_data:
                yaml_data['Outputs'] = {}
            for name, definition in zip(names, definitions):
                yaml_data['Outputs'][name] = definition
            return yaml_data
        self.pipeline.append(_add_exports)
        return self

    def get_params(self):
        def _get_params(yaml_data):
            try:
                return yaml_data['Parameters']
            except KeyError:
                return []
        self.pipeline.append(_get_params)
        return self

    def to_yaml(self):
        def _to_yaml(yaml_data):
            return yaml.dump(yaml_data, default_flow_style=False)
        self.pipeline.append(_to_yaml)
        return self


class CloudFormationTemplate:
    '''
    Provides validation of CloudFormation templates and holds nested CFN parameter formatting class
    '''
    class Parameter(object):

        def __init__(self, key, val, use_prev_val=True):
            self.key = key
            self.value = val
            self.use_prev_val = use_prev_val

        def to_cfn(self):
            return {
                'ParameterKey': self.key,
                'ParameterValue': self.value,
                'UsePreviousValue': self.use_prev_val
            }

    def __init__(self, template_string):
        self.template = template_string

    def validate(self, cfn_client=BotoClientFactory.client_for(Service.CLOUDFORMATION)):
        '''
        Validates the template using boto3, returning True/False.
        Difficult to tell if the validation is 'deeper' than simple YAML validation, i.e. if Cloudformation semantics
          are validated as well. See:
          http://boto3.readthedocs.io/en/latest/reference/services/cloudformation.html#CloudFormation.Client.validate_template

        Can raise botocore.client.ClientError in exceptional circumstances.

        :param cfn_client:
        :return:
        '''
        logger.debug('Validating...')
        try:
            cfn_client.validate_template(TemplateBody=self.template)
        except ClientError as ce:
            # Validation error is signaled via exception with these response elements/values
            if ce.response['Error']['Code'] == 'ValidationError':
                logger.error('Received ValidationError')
                logger.error(ce)
                return False
            else:
                # Some other type of error occured, so raise as exception
                print('Received unexpected botocore.client.ClientError code')
                raise ce
        else:
            return True


# TODO: Refactor to module-level method, or move into other class
class Stack:
    '''
    Represents a live stack. Wraps boto3 Resource API
    '''

    def __init__(self, stack):
        self.stack = stack

    @classmethod
    def from_stack_name(cls, stack_name, cloudformation=BotoResourceFactory.resource_for(Service.CLOUDFORMATION)):
        # creates the Stack resource, but does not fail if Stack does not exist
        stack_from_name = cloudformation.Stack(stack_name)
        try:
            status = stack_from_name.stack_status
        except ClientError:
            logger.error('Stack with that name doesnt exist')
            raise ValueError('Stack with name, {}, doesnt exist'.format(stack_name))

        if status != 'CREATE_COMPLETE':
            raise ValueError('Stack exists but is not in CREATE_COMPLETE state')

        return cls(stack=stack_from_name)
