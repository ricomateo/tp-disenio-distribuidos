import os
import json
import logging
from common.atomic_write import atomic_write

DEAD_CLIENTS_FILE = "dead_clients.json"
MAX_SIZE = 1000


class DeadClientsTracker:
    """
    Tracks the dead clients and performs cleanup
    of dead client files.
    """

    def __init__(self):
        """
        Tries to load the dead clients from the DEAD_CLIENTS_FILE,
        and looks for dead client state files to remove (and removes them if there are any)
        """
        self.max_size = MAX_SIZE
        try:
            with open(DEAD_CLIENTS_FILE, "r", encoding="utf-8") as f:
                self.dead_clients = json.loads(f.read())
        except Exception as e:
            logging.warning("Failed to read '%s' file. Error: %s", DEAD_CLIENTS_FILE, e)
            self.dead_clients = []

        self._remove_leftover_files()

    def set_client_as_dead(self, client_id):
        """
        Sets the given client as dead
        """
        self.dead_clients.append(client_id)
        # Delete stale data
        if len(self.dead_clients) > self.max_size:
            self.dead_clients = self.dead_clients[MAX_SIZE // 10 :]
        content = json.dumps(self.dead_clients)
        atomic_write(DEAD_CLIENTS_FILE, content)

    def client_is_dead(self, client_id):
        """
        Returns whether the given client is dead
        """
        return client_id in self.dead_clients

    def _remove_leftover_files(self):
        """
        Removes the left over client state files

        NOTE: the left over files are searched as 'client.*.json'.
        """
        # Look for left over files
        for dead_client in self.dead_clients:
            client_state_file = f"client.{dead_client}.json"
            if not os.path.exists(client_state_file):
                continue
            logging.debug(
                "Client %s is dead but %s was not removed",
                dead_client,
                client_state_file,
            )
            try:
                os.remove(client_state_file)
                logging.debug("Removed file %s", client_state_file)
            except Exception as e:
                logging.error(
                    "Failed to remove file '%s'. Error: %s", client_state_file, e
                )
