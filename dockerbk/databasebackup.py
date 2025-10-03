import subprocess
import re

class DatabaseBackup:
    """
    Class for managing database backups in Docker containers.
    """

    @staticmethod
    def stop_containers_with_suffix(suffix, exclude_container):
        """
        Stops all containers that start with the suffix, excluding the passed one.
        Returns the list of stopped containers.
        """
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            stdout=subprocess.PIPE,
            text=True,
            check=True
        )
        containers = result.stdout.strip().split('\n')
        to_stop = [c for c in containers if c.startswith(suffix) and c != exclude_container]
        for name in to_stop:
            subprocess.run(["docker", "stop", name], check=True)
        return to_stop

    @staticmethod
    def is_mariadb_version_below_11(version):
        """
        Returns True if the MariaDB version is below 11.0
        """
        try:
            # Handles versions like '10.11.2', '10.5', 'lts', etc.
            parts = version.split('.')
            major = int(parts[0]) if parts[0].isdigit() else 0
            return major < 11
        except Exception:
            return False

    @staticmethod
    def get_postgres_user(container_name):
        """
        Retrieves the PostgreSQL user from the container (POSTGRES_USER), default 'postgres'.
        """
        try:
            result = subprocess.run(
                ["docker", "exec", container_name, "printenv", "POSTGRES_USER"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            user = result.stdout.strip()
            return user if user else "postgres"
        except Exception:
            return "postgres"

    @staticmethod
    def get_db_root_password(container_name, db_type):
        """
        Retrieves the root password from the container via docker inspect.
        """
        env_var = "MYSQL_ROOT_PASSWORD" if db_type == "MySQL" else "MARIADB_ROOT_PASSWORD"
        try:
            result = subprocess.run(
                ["docker", "inspect", container_name, "--format", "{{range .Config.Env}}{{println .}}{{end}}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
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
        Also supports MYSQL_USER/MYSQL_PASSWORD variables.
        """
        if db_type == "MySQL":
            user_var = "MYSQL_USER"
            pass_var = "MYSQL_PASSWORD"
        else:
            user_var = "MARIADB_USER"
            pass_var = "MARIADB_PASSWORD"
        # Fallback to MySQL variables if MariaDB ones are not present
        fallback_user_var = "MYSQL_USER"
        fallback_pass_var = "MYSQL_PASSWORD"
        try:
            result = subprocess.run(
                ["docker", "inspect", container_name, "--format", "{{range .Config.Env}}{{println .}}{{end}}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            user = None
            password = None
            for line in result.stdout.splitlines():
                if line.startswith(user_var + "="):
                    user = line.split("=", 1)[1]
                elif line.startswith(pass_var + "="):
                    password = line.split("=", 1)[1]
                elif line.startswith(fallback_user_var + "=") and not user:
                    user = line.split("=", 1)[1]
                elif line.startswith(fallback_pass_var + "=") and not password:
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
        user, password = DatabaseBackup.get_db_user_password(container_name, db_type)
        if not user or not password:
            print("User or password not found for MariaDB/MySQL backup.")
            return False

        password_opt = ["-p" + password]

        if db_type == "MySQL" or db_type == "MariaDB":
            # Backup only the specified database if available
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
            user_pg = DatabaseBackup.get_postgres_user(container_name)
            cmd = [
                "docker", "exec", container_name,
                "pg_dumpall", "-U", user_pg
            ]
        elif db_type == "MongoDB":
            cmd = [
                "docker", "exec", container_name,
                "mongodump", "--archive"
            ]
        else:
            print(f"Backup not implemented for type: {db_type}")
            return False

        with open(backup_file, "wb") as f:
            proc = subprocess.run(cmd, stdout=f)
            return proc.returncode == 0

    @staticmethod
    def restart_containers(container_list):
        """
        Restarts the containers in the list.
        """
        for name in container_list:
            subprocess.run(["docker", "start", name], check=True)

    @staticmethod
    def manage_backup(container_name, db_type):
        """
        Stops containers with the same suffix, performs backup, and restarts them.
        """
        # suffix = container_name.split('-')[0]
        # stopped = DatabaseBackup.stop_containers_with_suffix(suffix, container_name)
        backup_file = f"{container_name}_{db_type}_backup.sql"
        print(f"Performing backup to {backup_file}...")
        ok = DatabaseBackup.backup_database(container_name, db_type, backup_file)
        if ok:
            print("Backup completed.")
        else:
            print("Backup failed.")
        # DatabaseBackup.restart_containers(stopped)
        print("Containers restarted.")

    @staticmethod
    def get_real_mariadb_version(container_name):
        """
        Returns the real MariaDB version from the container.
        """
        try:
            result = subprocess.run(
                ["docker", "exec", container_name, "mariadb", "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            # Example output: 'mariadb from 11.8.3-MariaDB, client 15.2 for debian-linux-gnu'
            match = re.search(r'from ([\d\.]+)-MariaDB', result.stdout)
            if match:
                return match.group(1)
        except Exception as e:
            print(f"Error retrieving MariaDB version: {e}")
        return "unknown"

if __name__ == "__main__":
    # Example test
    name = "firefly_iii_db"
    db_type = "MariaDB"
    DatabaseBackup.manage_backup(name, db_type)