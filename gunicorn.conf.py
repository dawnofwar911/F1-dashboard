# gunicorn.conf.py
# This file is used by Gunicorn to configure the server.
# It allows us to hook into the Gunicorn lifecycle to perform actions
# such as a graceful shutdown of our application.

import logging
import os

# We need to set up logging here as well, so that our shutdown messages are visible.
from app.utils import setup_logging
setup_logging()

from app.shutdown import controlled_shutdown

def worker_exit(server, worker):
    """
    Gunicorn hook that is called when a worker is exiting.
    This is the proper place to trigger our application's shutdown logic.
    """
    logger = logging.getLogger("F1App.Gunicorn.Hook")
    logger.info(f"Worker {worker.pid} is exiting. Triggering controlled shutdown.")
    controlled_shutdown()
    logger.info(f"Controlled shutdown complete for worker {worker.pid}.")

# You can also bind your settings here instead of using the command line
bind = "0.0.0.0:8050"
workers = 1
threads = 8
timeout = 120
