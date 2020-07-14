import argparse
import logging
import sys
import os
import traceback

from sqlsorcery import MSSQL
import xmltodict
import pandas as pd

from adaptive import Adaptive
from mailer import Mailer


parser = argparse.ArgumentParser()
parser.add_argument(
    "--version",
    help="Name of the version that data will be pulled from.",
    dest="version",
    nargs=1,
)
args = parser.parse_args()
VERSION = args.version[0]

logging.basicConfig(
    handlers=[
        logging.FileHandler(filename="app.log", mode="w+"),
        logging.StreamHandler(sys.stdout),
    ],
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S%p",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    try:
        mailer = Mailer()
        adaptive = Adaptive(VERSION)
        sql = MSSQL()
        # TODO export data
        xml = adaptive.export_data()
        mailer.notify(success=True)
    except:
        logging.error(traceback.format_exc())
        mailer.notify(success=False)


if __name__ == "__main__":
    main()
