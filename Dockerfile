FROM python:3.7.9-alpine3.12 AS venv-image
WORKDIR /usr/src/app

ENV PIP_VERSION="21.0"
ENV POETRY_VERSION="1.1.4"
RUN apk add --no-cache \
    file \
    make \
    build-base \
    curl \
    gcc \
    git \
    musl-dev \
    libffi-dev \
    python3-dev \
    postgresql-dev \
  && pip install --upgrade pip==$PIP_VERSION \
  && curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python \
  && ln -s "$HOME/.poetry/bin/poetry" "/usr/local/bin" \
  && python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false \
  && poetry install --no-dev --no-interaction


# This is the second and final image. Starting from a clean alpine
# image, it copies over the previously created virtual environment.
FROM python:3.7.9-alpine3.12 AS runtime-image
ARG FLASK_APP=swpt_accounts

ENV FLASK_APP=$FLASK_APP
ENV APP_ROOT_DIR=/usr/src/app
ENV APP_ASSOCIATED_LOGGERS=flask_signalbus.signalbus_cli
ENV PYTHONPATH="$APP_ROOT_DIR"
ENV PATH="/opt/venv/bin:$PATH"
ENV GUNICORN_LOGLEVEL=warning
ENV dramatiq_restart_delay=300

RUN apk add --no-cache \
    libffi \
    postgresql-libs \
    supervisor \
    gettext \
    && addgroup -S "$FLASK_APP" \
    && adduser -S -D -h "$APP_ROOT_DIR" "$FLASK_APP" "$FLASK_APP"

COPY --from=venv-image /opt/venv /opt/venv

WORKDIR /usr/src/app

COPY docker/entrypoint.sh \
     docker/gunicorn.conf.py \
     docker/supervisord.conf \
     docker/trigger_supervisor_process.py \
     wsgi.py \
     tasks.py \
     pytest.ini \
     ./
COPY migrations/ migrations/
COPY tests/ tests/
COPY $FLASK_APP/ $FLASK_APP/
RUN python -m compileall -x '^\./(migrations|tests)/' . \
    && rm -f .env \
    && chown -R "$FLASK_APP:$FLASK_APP" .

USER $FLASK_APP
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
CMD ["all"]
