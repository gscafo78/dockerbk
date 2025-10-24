#!/usr/bin/env python3

import logging
import argparse
from dockerbk.container import Container
from utils.logger import Logger
from dockerbk.databasebackup import DatabaseBackup

__version__ = "0.0.1"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"



if __name__ == "__main__":

    # --------------------------------------------------------------------------
    # 1) Parse command-line arguments
    # --------------------------------------------------------------------------
    parser = argparse.ArgumentParser(description="Docker Backup Utility")
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show program version and exit.",
    )
    parser.add_argument(
        "-d", "--directory",
        dest="backup_path",
        default=".",
        help="Path to backup files (default: .)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging (default is INFO).",
    )
    args = parser.parse_args()

    # --------------------------------------------------------------------------
    # 2) Configure root logger
    # --------------------------------------------------------------------------
    logging.basicConfig(format=LOG_FORMAT)
    logger = logging.getLogger(__name__)
    # Start with DEBUG if requested, otherwise INFO
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    log = logger or logging.getLogger(__name__)
    containers = Container(logger=log)
    dbbackup = DatabaseBackup(logger=log)
    containers_list = containers.get_running_containers()
    if not containers_list:
        print("No running containers found.")

    containers.stop_containers(db=False)
    for name in containers_list:
        is_db, db_type = containers.verify_database_type_from_image(name)
        if is_db:
            log.info(f"Container: {name}, DB Type: {db_type}")
            try:
                dbbackup.manage_backup(name, db_type, args.backup_path)
            except Exception as e:
                logger.error(f"Backup failed: {e}")
        else:
            log.debug(f"Container: {name} is not a supported database.")

    containers.stop_containers(db=True)
    log.info("Creating Docker volumes backup tar.gz...")
    containers.create_tar_gz(args.backup_path)
    containers.start_containers(db=True)
    containers.start_containers(db=False)

