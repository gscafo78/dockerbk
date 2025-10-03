import subprocess
import json
import logging

__version__ = "0.0.1"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__name__)


class Container:

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    @staticmethod
    def verify_database_type_from_image(container_name, logger=None):
        """
        Verifies the database type via the container's image.
        Returns a tuple (is_db: bool, db_type: str).
        """
        db_mapping = {
            "mysql": "MySQL",
            "postgres": "PostgreSQL",
            "mongo": "MongoDB",
            "redis": "Redis",
            "mariadb": "MariaDB",
            "oracle": "Oracle",
            "mssql": "SQL Server"
        }
        log = logger or logging.getLogger(__name__)
        try:
            result = subprocess.run(
                ["docker", "inspect", container_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            info = json.loads(result.stdout)
            image = info[0]["Config"]["Image"].lower()
            for key in db_mapping:
                if key in image:
                    return True, db_mapping[key]
            return False, None
        except Exception as e:
            log.error(f"Error inspecting container '{container_name}': {e}")
            return False, None

    @staticmethod
    def get_running_containers(logger=None):
        """
        Returns the list of names of running containers via Docker.
        """
        log = logger or logging.getLogger(__name__)
        try:
            result = subprocess.run(
                ["docker", "ps", "--format", "{{.Names}}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            containers = result.stdout.strip().split('\n')
            return [c for c in containers if c]
        except Exception as e:
            log.error(f"Error retrieving containers: {e}")
            return []


# Module-level functions for backward compatibility
get_running_containers = Container.get_running_containers
verify_database_type_from_image = Container.verify_database_type_from_image


if __name__ == "__main__":
    containers = Container.get_running_containers()
    if not containers:
        print("No running containers found.")
    for name in containers:
        is_db, db_type = Container.verify_database_type_from_image(name)
        if is_db:
            print(f"Container '{name}' is a database of type: {db_type}")
        else:
            print(f"Container '{name}' is NOT a database.")