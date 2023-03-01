import boto3

from rkstr8.cloud import Command
from rkstr8.clients import UnmarshallingContext, EventUnmarshaller, Event, queue_id_for_stage
from rkstr8.cloud.batch import BatchJobListStatusPoller


def get_poller(event):
    return Poller(
        event,
        UnmarshallingContext(),
        boto3.client('batch'),
    )


class Poller:

    def __init__(self, event,unmarshalling_context, batch_client):
        self.event = event
        self.event_api = None

        self.unmarshalling_context = unmarshalling_context
        self.batch_client = batch_client

    def _initialize_if_needed(self):

        if not self.event_api:
            self.event_api = Event(self.event)
            EventUnmarshaller(self.event_api, self.unmarshalling_context).unmarshall_to_context()

    def get_polling_action_and_context(self):

        self._initialize_if_needed()

        polling_action = PollingAction(
            poller=JobQueuePoller(self.batch_client, self.event_api, BatchJobListStatusPoller(job_ids=None))
        )

        return polling_action, self.unmarshalling_context


class PollingAction(Command):

    def __init__(self, poller: JobQueuePoller):
        self.poller = poller

    def execute(self, context):
        pipeline = context.pipeline

        return self.poller.poll(pipeline)


class JobQueuePoller:

    def __init__(self, batch_client, event_api, batch_poller):
        self.batch_client = batch_client
        self.event_api = event_api
        self.batch_poller = batch_poller

    def poll(self, pipeline):

        stage = pipeline.get_stage(self.event_api.get_curr_stage())
        queue_id = queue_id_for_stage(self.event_api, stage)

        return self.batch_poller.polling_outcome_for_queue(queue_id)
