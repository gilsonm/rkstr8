FROM python:3.9-slim

WORKDIR /home

ARG TASK_DEF_SCRIPT=demo.pipeline.taskdefs.py
ARG TASK_DEF_REQS=demo.pipeline.taskdefs.requirements.txt
ARG RKSTR8_WHEEL=rkstr8-0.0.1-py3-none-any.whl

COPY ${RKSTR8_WHEEL} .
COPY ${TASK_DEF_SCRIPT} .
COPY ${TASK_DEF_REQS} .

RUN pip install --upgrade pip
RUN pip install ${RKSTR8_WHEEL}
RUN pip install -r ${TASK_DEF_REQS}