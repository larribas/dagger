FROM python:3.8.7-slim

ARG WHEEL

COPY dist/$WHEEL /tmp

RUN pip --disable-pip-version-check install /tmp/$WHEEL

WORKDIR /usr/src/app
ENV PYTHONPATH=/usr/src/app
COPY examples examples
