import os

accesslog = "-" if os.getenv("APP_LOG_LEVEL") == "debug" else None
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s %(D)s "%({X-Logging-Context}o)s" "%(f)s" "%(a)s"'
disable_redirect_access_to_syslog = True

for k, v in os.environ.items():
    if k.startswith("GUNICORN_"):
        key = k.split('_', 1)[1].lower()
        locals()[key] = v
