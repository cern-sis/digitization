FROM python:3.9-slim

WORKDIR /code

ENV PATH="/root/.poetry/bin:${PATH}"

RUN apt-get update \
 && apt-get install -y kstart curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && curl -sSL https://install.python-poetry.org/ \
  | python

ENV PATH="/root/.local/bin:$PATH"

RUN poetry --version

COPY poetry.lock pyproject.toml ./
COPY digitization ./digitization

RUN poetry install
