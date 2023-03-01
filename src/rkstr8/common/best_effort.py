APP = 'app'
BEST_EFFORT = 'best_effort'
AGGS = 'aggs'
STAGES = 'stages'


class AggregateNodeColoring:

    def __init__(self, status):
        self.status = status

    def to_item(self):
        if not self.status:
            raise ValueError('All aggregate nodes must be colored by Status')
        else:
            return {
                'Status': self.status
            }

    @classmethod
    def from_item(cls, item):
        try:
            status = item['Status']
        except KeyError:
            raise ValueError('All aggregate nodes must be colored by Status')
        return cls(status)


class StageNodeColoring:

    def __init__(self, job_id, queue_id):
        self.job_id = job_id
        self.queue_id = queue_id

    def to_item(self):
        item = dict()
        if self.job_id:
            item['JobId'] = self.job_id
        if self.queue_id:
            item['QueueId'] = self.queue_id
        return item

    @classmethod
    def from_item(cls, item):
        try:
            job_id = item['JobId']
        except KeyError:
            job_id = None

        try:
            queue_id = item['QueueId']
        except KeyError:
            queue_id = None

        return cls(job_id, queue_id)


class DynamoDbFacade:

    def __init__(self, dynamo_db):
        self.dynamo_db = dynamo_db

    def put_stage_coloring(self, stage_name, coloring):

        return self.dynamo_db.put_record(
            namespace=APP,
            partition=BEST_EFFORT,
            content_parts=[STAGES, stage_name],
            item=coloring.to_item()
        )

    def get_stage_coloring(self, stage_name):

        item = self.dynamo_db.get_record(
            namespace=APP,
            partition=BEST_EFFORT,
            content_parts=[STAGES, stage_name]
        )

        return StageNodeColoring.from_item(item)

    def put_agg_coloring(self, agg_name, coloring):

        return self.dynamo_db.put_record(
            namespace=APP,
            partition=BEST_EFFORT,
            content_parts=[AGGS, agg_name],
            item=coloring.to_item()
        )

    def get_agg_coloring(self, agg_name):

        item = self.dynamo_db.get_record(
            namespace=APP,
            partition=BEST_EFFORT,
            content_parts=[AGGS, agg_name]
        )

        return AggregateNodeColoring.from_item(item)
