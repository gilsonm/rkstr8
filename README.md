# RKSTR8

RKSTR8 (read *"orchestrate"*) is a general purpose, scalable, workflow definition language and execution platform for AWS.

Originally developed for large-scale data- and CPU-intensive computational biology workflows (aka pipelines), it just
as well supports deep learning GPU-intensive pipelines and general batch processing workflows.

RKSTR8 entirely removes the operations burden of distributed computing (i.e. designing, setting up and tearing down networking, compute resources, security 
controls etc. in your cloud environment) so that you can focus on writing the functional components of your workflows.

At minimum, one need only specify the number of CPUs (and/or GPUs) and memory required for each step of your workflow.

Workflow steps are implemented by python functions. Packaged in Docker images, these functions can run any external 
programs they need to complete their work.

Steps are connected by dependencies that are specificied in a declarative language in the easy-to-read and easy-to-edit YAML format.

One need only an AWS account and can simply `pip install rkstr8` and start writing workflows, even from the AWS Cloud Shell in the web browser.


Many workflows can be run in parallel on the same AWS account. Multiple workflows can combined in complex, non-linear ways by importing
RKSTR8 as a python module and using the simple `launch` API.
