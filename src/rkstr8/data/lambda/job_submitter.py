from rkstr8.clients.best_effort import get_client
import yaml

def handler(event, context):
    print('Submitter:')
    print(yaml.dump(event, default_flow_style=False))

    client = get_client(event)
    status = client.schedule_one_round()

    return status
