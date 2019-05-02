FROM python:3.7-alpine
WORKDIR /usr/src/app

ARG FLASK_APP=swpt_accounts

ENV APP_ROOT_DIR=/usr/src/app
ENV APP_LOGGING_CONFIG_FILE=/usr/src/app/swpt_accounts/logging.conf
ENV PYTHONPATH=/usr/src/app
ENV FLASK_APP=$FLASK_APP

ENV POETRY_VERSION="0.12.14"
ENV USE_PIP_VERSION="19.1"
ENV USE_GUNICORN_VERSION="19.9.0"

# Gunicorn's default number of workers.
ENV WEB_CONCURRENCY=2

RUN apk add --no-cache \
    curl \
    gcc \
    musl-dev \
    python3-dev \
    postgresql-dev \
    git \
    supervisor \
  && pip install --upgrade pip==$USE_PIP_VERSION \
  && pip install gunicorn==$USE_GUNICORN_VERSION \
  && curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python \
  && ln -s "$HOME/.poetry/bin/poetry" "/usr/local/bin"

COPY pyproject.toml poetry.lock ./
RUN poetry config settings.virtualenvs.create false \
  && poetry install --no-dev --no-interaction

COPY docker/ wsgi.py tasks.py ./
COPY $FLASK_APP/ $FLASK_APP/
RUN python -m compileall .
COPY migrations/ migrations/

# Ensure no .env file is present.
RUN rm -f .env

ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
CMD ["serve"]
