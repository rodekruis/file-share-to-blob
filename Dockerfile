FROM python:3.9-slim-bullseye
WORKDIR /pipeline
ADD pipeline .
RUN pip install .
