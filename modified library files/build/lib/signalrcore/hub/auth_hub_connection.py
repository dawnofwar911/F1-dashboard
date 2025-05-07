from .base_hub_connection import BaseHubConnection
from ..helpers import Helpers
import logging
import threading

class AuthHubConnection(BaseHubConnection):
    def __init__(self, auth_function, headers=None, **kwargs):
        if headers is None:
            self.headers = dict()
        else:
            self.headers = headers
        self.auth_function = auth_function
        super(AuthHubConnection, self).__init__(headers=headers, **kwargs)

    def start(self):
        # Use self.logger if available, otherwise fallback
        logger = getattr(self, 'logger', logging.getLogger(__name__))
        #logger.debug(f"THREAD {threading.get_ident()}: ENTERING AuthHubConnection.start")
   
        try:
            # --- Check and handle auth function ---
            if self.auth_function and callable(self.auth_function):
                logger.debug("Auth function provided, attempting to get token.")
                try:
                    self.token = self.auth_function() # Call only if it exists and is callable
                except Exception as auth_ex:
                     logger.error(f"Error executing auth_function: {auth_ex}", exc_info=True)
                     # Decide how to handle auth error - raise? set state?
                     raise HubConnectionError(f"Auth function failed: {auth_ex}") from auth_ex

                if self.token is not None and isinstance(self.token, str): # Only update headers if valid token received
                     # Ensure headers dictionary exists
                     if not isinstance(self.headers, dict): self.headers = {}
                     self.headers["Authorization"] = "Bearer " + self.token
                     logger.debug("Authorization header set with token.")
                else:
                     logger.warning("Auth function did not return a valid token string.")
                     # Remove potentially stale header if token is now None
                     if isinstance(self.headers, dict) and "Authorization" in self.headers:
                          del self.headers["Authorization"]
            else:
                # No auth function provided or it's None
                logger.debug("No auth function provided, skipping token acquisition.")
                self.token = None # Ensure token is None
                # Remove auth header if it somehow exists without an auth function
                if isinstance(self.headers, dict) and "Authorization" in self.headers:
                     logger.warning("Removing Authorization header as no auth function is set.")
                     del self.headers["Authorization"]
            # --- End check ---

            # --- Call parent's start method ---
            # This is where the transport connection is actually initiated
            logger.debug("Calling parent start method to initiate transport connection...")
            # Use super() without arguments in Python 3
            return super().start()
            # --- End parent call ---

        except Exception as ex:
            # Catch errors during the start process (including auth or parent start)
            logger.error(f"Error during AuthHubConnection start sequence: {ex}", exc_info=True)
            # Raise the exception to be caught by the calling thread (connect function)
            raise ex # Re-raise the original exception or a HubConnectionError
