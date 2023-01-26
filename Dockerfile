FROM python:3.8

WORKDIR /code

ENV PATH="/root/.poetry/bin:${PATH}"

ARG POETRY_VERSION
ENV POETRY_VERSION="${POETRY_VERSION:-1.1.6}"
RUN apt-get update \
 && apt-get install -y kstart \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && curl -sSL https://install.python-poetry.org/ \
  | python - --version "${POETRY_VERSION}"

ENV PATH="/root/.local/bin:$PATH"

RUN poetry --version

COPY poetry.lock pyproject.toml ./
COPY cli.py ./

RUN poetry install
