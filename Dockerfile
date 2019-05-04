FROM python:3.7.3-alpine3.9 AS compile-image
WORKDIR /usr/src/app

ENV PIP_VERSION="19.1"
ENV POETRY_VERSION="0.12.14"
RUN apk add --no-cache \
    curl \
    gcc \
    git \
    musl-dev \
    libffi-dev \
    postgresql-dev \
  && pip install --upgrade pip==$PIP_VERSION \
  && curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python \
  && ln -s "$HOME/.poetry/bin/poetry" "/usr/local/bin" \
  && python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"
COPY pyproject.toml poetry.lock ./
RUN poetry config settings.virtualenvs.create false \
  && poetry install --no-dev --no-interaction


# This is the second and final image. Starting from a clean alpine
# image, it copies over the previously created virtual environment.
FROM python:3.7.3-alpine3.9 AS runtime-image
WORKDIR /usr/src/app
ARG FLASK_APP=swpt_accounts

RUN apk add --no-cache \
    libffi \
    postgresql-libs \
    supervisor

ENV APP_ROOT_DIR=/usr/src/app
ENV APP_LOGGING_CONFIG_FILE="$APP_ROOT_DIR/swpt_accounts/logging.conf"
ENV FLASK_APP=$FLASK_APP
ENV PYTHONPATH="$APP_ROOT_DIR"
ENV PATH="/opt/venv/bin:$PATH"

COPY --from=compile-image /opt/venv /opt/venv
COPY docker/ wsgi.py tasks.py ./
RUN rm -f .env
COPY migrations/ migrations/
COPY $FLASK_APP/ $FLASK_APP/
RUN python -m compileall -x '^\./migrations/' .

ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
CMD ["serve"]
