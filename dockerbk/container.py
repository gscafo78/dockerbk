import subprocess
import json
import logging
import tarfile
import os
from datetime import datetime

__version__ = "0.0.1"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__name__)


class Container:

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.running_containers = self.get_running_containers(self.logger)
        self.app_containers = []
        self.db_containers = []
        self.get_typed_containers(self.logger)

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

    def get_typed_containers(self, logger=None):
        """
        Populates db_containers and app_containers lists.
        """
        log = logger or self.logger
        for name in self.running_containers:
            is_db, db_type = Container.verify_database_type_from_image(name, log)
            if is_db:
                self.db_containers.append({"name": name, "db_type": db_type})
                log.info(f"Container '{name}' is a database of type: {db_type}")
            else:
                self.app_containers.append(name)
                log.info(f"Container '{name}' is NOT a database.")


    def start_containers(self, db=True):
        """
        Starts containers based on type (db or app).
        Returns the list of started containers.
        """
        log = self.logger
        log.info("Starting containers...")
        # Get containers to consider
        if db:
            containers = [c['name'] for c in self.db_containers]
        else:
            containers = self.app_containers
        # Start each container
        for name in containers:
            log.info(f"Starting container: {name}")
            subprocess.run(["docker", "start", name], check=True)
        log.info(f"Started {len(containers)} containers")
        return containers


    def stop_containers(self, db=False, suffix=None, exclude_container=None):
        """
        Stops containers based on type (db or app), suffix, and exclude_container.
        Returns the list of stopped containers.
        """
        log = self.logger
        log.info("Stopping containers...")
        # Get containers to consider
        if db:
            containers = [c['name'] for c in self.db_containers]
        else:
            containers = self.app_containers
        # Filter based on suffix and exclude_container
        if suffix:
            to_stop = [c for c in containers if c.startswith(suffix) and (exclude_container is None or c != exclude_container)]
        else:
            to_stop = [c for c in containers if exclude_container is None or c != exclude_container]
        # Stop each filtered container
        for name in to_stop:
            log.info(f"Stopping container: {name}")
            subprocess.run(["docker", "stop", name], check=True)
        log.info(f"Stopped {len(to_stop)} containers")
        return to_stop

    @staticmethod
    def create_tar_gz(dest_dir=".", source_dir="/var/lib/docker/volumes", logger=None):
        """
        Creates a tar.gz archive of the specified source directory.
        """
        log = logger or logging.getLogger(__name__)
        log.info(f"Starting backup of Docker volumes from {source_dir}")
        if not os.path.exists(source_dir):
            log.error(f"Source directory {source_dir} does not exist")
            raise ValueError(f"Source directory {source_dir} does not exist")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"docker_volumes_backup_{timestamp}.tar.gz"
        archive_path = os.path.join(dest_dir, archive_name)
        log.info(f"Creating archive {archive_path}")
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))
        log.info(f"Backup completed successfully: {archive_path}")
        return archive_path


if __name__ == "__main__":
    containers = Container()
    if not containers.running_containers:
        print("No running containers found.")
    else:
        print("Running containers found:")
        print("\nDatabase Containers:")
        for db in containers.db_containers:
            print(f"- {db['name']} ({db['db_type']})")
        print("\nApplication Containers:")
        for app in containers.app_containers:
            print(f"- {app}")
        containers.stop_containers(db=False)