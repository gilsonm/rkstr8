from rkstr8.clients.container import pipeline_task
import cowsay

@pipeline_task
def hello_task(task_context):
    cowsay.cow('Hello, world!')

hello_task()
