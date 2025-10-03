#!/usr/bin/env python3

import logging
import argparse
from dockerbk.container import Container
from utils.logger import Logger

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
    # parser.add_argument(
    #     "-c", "--config",
    #     dest="config_path",
    #     default="./settings.json",
    #     help="Path to configuration file (default: ./settings.json)",
    # )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG-level logging (default is INFO).",
    )
    args = parser.parse_args()

    # --------------------------------------------------------------------------
    # 2) Configure root logger
    # --------------------------------------------------------------------------
    logging.basicConfig(format=LOG_FORMAT)
    logger = logging.getLogger(__name__)
    # Start with DEBUG if requested, otherwise INFO
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)

    containers = Container()
    containers_list = containers.get_running_containers()
    if not containers_list:

        print("No running containers found.")
    for name in containers_list:
        is_db, db_type = containers.verify_database_type_from_image(name)
        if is_db:
            print(f"Container: {name}, DB Type: {db_type}")
        else:
            print(f"Container: {name} is not a supported database.")
