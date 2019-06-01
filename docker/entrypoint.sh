#!/bin/sh
set -e

# During development, we should be able to connect to services
# installed on "localhost" from the container. To allow this, we find
# the IP address of the docker host, and then in the value of each
# variable which name ends with "_URL" we substitute "localhost" with
# that IP address.
host_ip=$(ip route show | awk '/default/ {print $3}')
for envvar_name in $(env | grep -oE '^[A-Z_]+_URL\b'); do
    eval envvar_value=\$$envvar_name
    eval export $envvar_name=$(echo "$envvar_value" | sed -E "s/(.*@|.*\/\/)localhost\b/\1$host_ip/")
done

# This function tries to upgrade the database schema with exponential
# backoff. This is necessary during development, because the database
# might not be running yet when this script executes.
perform_db_upgrade() {
    local retry_after=1
    local time_limit=$(($retry_after << 5))
    local error_file="$APP_ROOT_DIR/flask-db-upgrade.error"
    echo 'Running database schema upgrade ...'
    while [[ $retry_after -lt $time_limit ]]; do
        if flask db upgrade 2>$error_file; then
            return 0
        fi
        sleep $retry_after
        retry_after=$((2 * retry_after))
    done
    cat "$error_file"
    return 1
}

# This function is intended to perform additional one-time
# initializations. Make sure that it is idempotent.
# (https://en.wikipedia.org/wiki/Idempotence)
perform_initializations() {
    return 0
}

case $1 in
    develop-run-flask)
        shift
        perform_db_upgrade
        perform_initializations
        flask signalbus flush -w 0
        exec flask run --host=0.0.0.0 --port $PORT --without-threads "$@"
        ;;
    develop-run-tasks)
        shift
        perform_db_upgrade
        perform_initializations
        exec dramatiq --processes ${DRAMATIQ_PROCESSES-4} --threads ${DRAMATIQ_THREADS-8} "$@"
        ;;
    test)
        perform_db_upgrade
        perform_initializations
        exec pytest
        ;;
    configure)
        flask db upgrade
        perform_initializations
        ;;
    serve)
        exec gunicorn --config "$APP_ROOT_DIR/gunicorn.conf" -b :$PORT wsgi:app
        ;;
    supervisord)
        exec supervisord -c "$APP_ROOT_DIR/supervisord.conf"
        ;;
    tasks)
        shift
        exec dramatiq --processes ${DRAMATIQ_PROCESSES-4} --threads ${DRAMATIQ_THREADS-8} "$@"
        ;;
    tasks-gevent)
        shift
        exec dramatiq-gevent --processes ${DRAMATIQ_PROCESSES-4} --threads ${DRAMATIQ_GREENLETS-8} "$@"
        ;;
    *)
        exec "$@"
        ;;
esac
