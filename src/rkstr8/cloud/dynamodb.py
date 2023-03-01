import hashlib
import yaml
from botocore.exceptions import ClientError


class DynamoDb:

    def __init__(self, table_name, dynamo_db_resource):
        self.table = dynamo_db_resource.Table(table_name)

    def _record_id_content_hash(self, namespace, partition, content_parts):
        record_id_parts = [namespace, partition] + content_parts
        content_str = '#'.join(map(str, record_id_parts)).encode('utf-8')
        digest = hashlib.md5(content_str).hexdigest()
        return digest

    def put_record(self, namespace, partition, content_parts, item):
        record_id = self._record_id_content_hash(namespace, partition, content_parts)
        item['RecordId'] = record_id
        try:
            response = self.table.put_item(Item=item)
        except ClientError as ce:
            print(ce.response['Error']['Message'])
            raise ce
        else:
            print(yaml.dump(response, default_flow_style=True))
            return True

    def get_record(self, namespace, partition, content_parts):
        record_id = self._record_id_content_hash(namespace, partition, content_parts)
        try:
            response = self.table.get_item(Key={'RecordId': record_id})
        except ClientError as ce:
            print(ce.response['Error']['Message'])
            raise ce
        else:
            print(yaml.dump(response, default_flow_style=True))
            item_or_none = response['Item'] if 'Item' in response else None
            return item_or_none


