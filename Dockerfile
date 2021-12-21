FROM python:3.8

WORKDIR /code

ENV PATH="/root/.poetry/bin:${PATH}"

ARG POETRY_VERSION
ENV POETRY_VERSION="${POETRY_VERSION:-1.1.6}"
RUN apt-get update \
 && apt-get install -y kstart \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py \
  | python - --version "${POETRY_VERSION}" \
 && poetry --version


COPY poetry.lock pyproject.toml ./
COPY cli.py ./

RUN poetry install
