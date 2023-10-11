FROM python:3.11-slim
WORKDIR /pipeline
ADD pipeline .
RUN pip install .
