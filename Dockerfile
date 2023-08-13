FROM python:3.10.6-alpine3.16 AS venv-image
WORKDIR /usr/src/app

ENV POETRY_VERSION="1.4.2"
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
    openssl-dev \
    cargo \
  && curl -sSL https://install.python-poetry.org | python - \
  && ln -s "$HOME/.local/bin/poetry" "/usr/local/bin"

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false --local \
  && python -m venv /opt/venv \
  && source /opt/venv/bin/activate \
  && poetry install --only main --no-interaction


# This is the second and final image. Starting from a clean alpine
# image, it copies over the previously created virtual environment.
FROM python:3.10.6-alpine3.16 AS app-image
ARG FLASK_APP=swpt_accounts

ENV FLASK_APP=$FLASK_APP
ENV APP_ROOT_DIR=/usr/src/app
ENV APP_ASSOCIATED_LOGGERS="swpt_pythonlib.flask_signalbus.signalbus_cli swpt_pythonlib.multiproc_utils"
ENV PYTHONPATH="$APP_ROOT_DIR"
ENV PATH="/opt/venv/bin:$PATH"
ENV GUNICORN_LOGLEVEL=warning
ENV SQLALCHEMY_SILENCE_UBER_WARNING=1

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
     pytest.ini \
     ./
COPY migrations/ migrations/
COPY $FLASK_APP/ $FLASK_APP/
RUN python -m compileall -x '^\./(migrations|tests)/' . \
    && rm -f .env \
    && chown -R "$FLASK_APP:$FLASK_APP" .

USER $FLASK_APP
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
CMD ["all"]
