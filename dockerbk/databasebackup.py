# Import necessary modules for subprocess execution, regular expressions, logging, system operations, and argument parsing
import subprocess
import re
import logging
import sys
import argparse

class DatabaseBackup:
    """
    Class for managing database backups in Docker containers.
    """
    # Define database type constants for supported database systems
    MYSQL = "MySQL"
    MARIADB = "MariaDB"
    POSTGRESQL = "PostgreSQL"
    MONGODB = "MongoDB"

    @staticmethod
    def stop_containers_with_suffix(suffix, exclude_container):
        """
        Stops all containers that start with the suffix, excluding the passed one.
        Returns the list of stopped containers.
        """
        # Run docker ps to get list of running container names
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            stdout=subprocess.PIPE,
            text=True,
            check=True
        )
        # Split output into list of container names
        containers = result.stdout.strip().split('\n')
        # Filter containers that start with the suffix but exclude the specified container
        to_stop = [c for c in containers if c.startswith(suffix) and c != exclude_container]
        # Stop each filtered container
        for name in to_stop:
            subprocess.run(["docker", "stop", name], check=True)
        return to_stop

    @staticmethod
    def is_mariadb_version_below_11(version):
        """
        Returns True if the MariaDB version is below 11.0
        """
        try:
            # Split version string by dots to extract parts
            parts = version.split('.')
            # Convert first part to int if it's a digit, else default to 0
            major = int(parts[0]) if parts[0].isdigit() else 0
            # Check if major version is less than 11
            return major < 11
        except Exception:
            # Return False on any parsing error
            return False

    @staticmethod
    def get_postgres_user(container_name):
        """
        Retrieves the PostgreSQL user from the container (POSTGRES_USER), default 'postgres'.
        """
        try:
            # Execute docker exec to get POSTGRES_USER environment variable
            result = subprocess.run(
                ["docker", "exec", container_name, "printenv", "POSTGRES_USER"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            # Strip whitespace from output
            user = result.stdout.strip()
            # Return user if found, else default to 'postgres'
            return user if user else "postgres"
        except Exception:
            # Return default on error
            return "postgres"

    @staticmethod
    def get_db_root_password(container_name, db_type):
        """
        Retrieves the root password from the container via docker inspect.
        """
        # Determine the environment variable name based on database type
        env_var = "MYSQL_ROOT_PASSWORD" if db_type == "MySQL" else "MARIADB_ROOT_PASSWORD"
        try:
            # Run docker inspect to get environment variables
            result = subprocess.run(
                ["docker", "inspect", container_name, "--format", "{{range .Config.Env}}{{println .}}{{end}}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            # Parse each line to find the password variable
            for line in result.stdout.splitlines():
                if line.startswith(env_var + "="):
                    return line.split("=", 1)[1]
            return None
        except Exception:
            return None

    @staticmethod
    def get_db_user_password(container_name, db_type):
        """
        Retrieves user and password from the container via docker inspect.
        Supports MySQL, MariaDB, and PostgreSQL.
        """
        # Set variable names based on database type
        if db_type == "MySQL":
            user_var = "MYSQL_USER"
            pass_var = "MYSQL_PASSWORD"
            fallback_user_var = "MARIADB_USER"
            fallback_pass_var = "MARIADB_PASSWORD"
        elif db_type == "MariaDB":
            user_var = "MARIADB_USER"
            pass_var = "MARIADB_PASSWORD"
            fallback_user_var = "MYSQL_USER"
            fallback_pass_var = "MYSQL_PASSWORD"
        elif db_type == "PostgreSQL":
            user_var = "POSTGRES_USER"
            pass_var = "POSTGRES_PASSWORD"
            fallback_user_var = None
            fallback_pass_var = None
        else:
            return None, None
        try:
            # Inspect container environment variables
            result = subprocess.run(
                ["docker", "inspect", container_name, "--format", "{{range .Config.Env}}{{println .}}{{end}}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            user = None
            password = None
            # Parse environment variables
            for line in result.stdout.splitlines():
                if line.startswith(user_var + "="):
                    user = line.split("=", 1)[1]
                elif line.startswith(pass_var + "="):
                    password = line.split("=", 1)[1]
                elif fallback_user_var and line.startswith(fallback_user_var + "=") and not user:
                    user = line.split("=", 1)[1]
                elif fallback_pass_var and line.startswith(fallback_pass_var + "=") and not password:
                    password = line.split("=", 1)[1]
            return user, password
        except Exception:
            return None, None

    @staticmethod
    def backup_database(container_name, db_type, backup_file):
        """
        Performs the database backup based on the type.
        Always uses the user and password specified in environment variables for MariaDB/MySQL.
        """
        # Retrieve user and password from container environment
        user, password = DatabaseBackup.get_db_user_password(container_name, db_type)
        if db_type in ("MySQL", "MariaDB") and (not user or not password):
            print("User or password not found for MariaDB/MySQL backup.")
            return False

        # Prepare password option for command
        password_opt = ["-p" + password]

        if db_type == "MySQL" or db_type == "MariaDB":
            # Attempt to get the specific database name from environment
            db_name = None
            try:
                result = subprocess.run(
                    ["docker", "inspect", container_name, "--format", "{{range .Config.Env}}{{println .}}{{end}}"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True
                )
                for line in result.stdout.splitlines():
                    if line.startswith("MYSQL_DATABASE="):
                        db_name = line.split("=", 1)[1]
                    elif line.startswith("MARIADB_DATABASE=") and not db_name:
                        db_name = line.split("=", 1)[1]
            except Exception:
                db_name = None

            # Construct mysqldump or mariadb-dump command
            if db_type == "MySQL":
                cmd = [
                    "docker", "exec", container_name,
                    "mysqldump", "-u", user, *password_opt, db_name if db_name else "--all-databases"
                ]
            else:  # MariaDB
                cmd = [
                    "docker", "exec", container_name,
                    "mariadb-dump", "-u", user, *password_opt, db_name if db_name else "--all-databases"
                ]
        elif db_type == "PostgreSQL":
            # Get PostgreSQL user and password
            user_pg, password_pg = DatabaseBackup.get_db_user_password(container_name, db_type)
            if not user_pg:
                user_pg = "postgres"
            cmd = [
                "docker", "exec", container_name,
                "pg_dumpall", "-U", user_pg
            ]
            if password_pg:
                # Set PGPASSWORD environment variable for password
                cmd.insert(2, "-e")
                cmd.insert(3, f"PGPASSWORD={password_pg}")
        elif db_type == "MongoDB":
            # Construct mongodump command for MongoDB
            cmd = [
                "docker", "exec", container_name,
                "mongodump", "--archive"
            ]
        else:
            print(f"Backup not implemented for type: {db_type}")
            return False

        # Execute the backup command and write output to file
        with open(backup_file, "wb") as f:
            proc = subprocess.run(cmd, stdout=f)
            return proc.returncode == 0

    @staticmethod
    def restart_containers(container_list):
        """
        Restarts the containers in the list.
        """
        # Start each container in the list
        for name in container_list:
            subprocess.run(["docker", "start", name], check=True)

    @staticmethod
    def manage_backup(container_name,
                      db_type,
                      path="."):
        """
        Performs database backup for the specified container and type.
        """
        # Determine backup file name based on database type
        if db_type in (DatabaseBackup.MYSQL, DatabaseBackup.MARIADB, DatabaseBackup.POSTGRESQL):
            backup_file = f"{container_name}_{db_type}_backup.sql"
        elif db_type == DatabaseBackup.MONGODB:
            backup_file = f"{container_name}_{db_type}_backup.archive"
        else:
            raise ValueError(f"Unsupported db_type: {db_type}")
        # Log the backup operation
        logger.info(f"Performing backup to {path}/{backup_file}...")
        tmp_filename = f"{path}/{backup_file}"
        # Perform the backup
        ok = DatabaseBackup.backup_database(container_name, db_type, tmp_filename)
        if ok:
            logger.info("Backup completed.")
        else:
            logger.error("Backup failed.")
            raise RuntimeError("Backup failed")

    @staticmethod
    def get_real_mariadb_version(container_name):
        """
        Returns the real MariaDB version from the container.
        """
        try:
            # Execute mariadb --version inside the container
            result = subprocess.run(
                ["docker", "exec", container_name, "mariadb", "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            # Use regex to extract version number from output
            match = re.search(r'from ([\d\.]+)-MariaDB', result.stdout)
            if match:
                return match.group(1)
        except Exception as e:
            print(f"Error retrieving MariaDB version: {e}")
        return "unknown"

# Configure logging to INFO level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # Set up command-line argument parser
    parser = argparse.ArgumentParser(description="Backup database from Docker container")
    parser.add_argument(
        "-c", "--container-name",
        dest="container_name",
        required=True,
        help="Name of the Docker container")
    parser.add_argument(
        "-dt", "--db-type",
        dest="db_type", choices=[DatabaseBackup.MYSQL, DatabaseBackup.MARIADB, DatabaseBackup.POSTGRESQL, DatabaseBackup.MONGODB], help="Database type")
    
    args = parser.parse_args()
    try:
        # Execute the backup management
        DatabaseBackup.manage_backup(args.container_name, args.db_type)
    except Exception as e:
        logger.error(f"Backup failed: {e}")
        sys.exit(1)