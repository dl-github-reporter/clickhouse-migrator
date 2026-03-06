FROM python:3.12.12-slim

ENV APP_DIR=/usr/app
ENV POETRY_VERSION=2.3.2
ENV POETRY_CACHE_DIR=$APP_DIR

RUN python3 -m pip install --upgrade pip && pip install "poetry==$POETRY_VERSION"

RUN groupadd -g 10666 python && \
    useradd -r -u 10666 -g python python && \
    mkdir /usr/app && \
    chown python:python $APP_DIR

WORKDIR $APP_DIR

COPY --chown=python:python . .

RUN poetry build && poetry install

USER 10666

ENTRYPOINT ["poetry", "run", "python", "src/clickhouse_migrator/cli.py"]
CMD ["--help"]
