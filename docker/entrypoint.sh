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
    echo -n 'Running database schema upgrade ...'
    while [[ $retry_after -lt $time_limit ]]; do
        if flask db upgrade 2>$error_file; then
            perform_db_initialization
            echo ' done.'
            return 0
        fi
        sleep $retry_after
        retry_after=$((2 * retry_after))
    done
    echo
    cat "$error_file"
    return 1
}

setup_rabbitmq_bindings() {
    flask swpt_accounts subscribe swpt_accounts
    return 0
}

# This function is intended to perform additional one-time database
# initialization. Make sure that it is idempotent.
# (https://en.wikipedia.org/wiki/Idempotence)
perform_db_initialization() {
    return 0
}

case $1 in
    develop-run-flask)
        shift
        exec flask run --host=0.0.0.0 --port $PORT --without-threads "$@"
        ;;
    test)
        perform_db_upgrade
        exec pytest
        ;;
    configure)
        perform_db_upgrade
        setup_rabbitmq_bindings
        ;;
    webserver)
        export GUNICORN_LOGLEVEL=${WEBSERVER_LOGLEVEL:-warning}
        export GUNICORN_WORKERS=${WEBSERVER_WORKERS:-1}
        export GUNICORN_THREADS=${WEBSERVER_THREADS:-3}
        exec gunicorn --config "$APP_ROOT_DIR/gunicorn.conf.py" -b :$PORT wsgi:app
        ;;
    protocol)
        exec dramatiq --processes ${PROTOCOL_PROCESSES-1} --threads ${PROTOCOL_THREADS-3} tasks:protocol_broker
        ;;
    process_chores)
        exec dramatiq --processes ${CHORES_PROCESSES-1} --threads ${CHORES_THREADS-3} tasks:chores_broker
        ;;
    process_balance_changes |process_transfer_requests | process_finalization_requests | scan_accounts \
        | scan_prepared_transfers)
        exec flask swpt_accounts "$@"
        ;;
    all)
        exec supervisord -c "$APP_ROOT_DIR/supervisord.conf"
        ;;
    *)
        exec "$@"
        ;;
esac
