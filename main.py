import argparse
import logging
from io import StringIO
import sys
import os
import traceback

from sqlsorcery import MSSQL
import xmltodict
import pandas as pd

from adaptive import Adaptive
from mailer import Mailer


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


def extract_data_from_parsed_dict(parsed_dict):
    """Pull csv-shaped data from parsed dictionary and write to file."""
    data = parsed_dict["response"]["output"]
    data.lstrip("![CDATA[").rstrip("]]")
    df = pd.read_csv(StringIO(data))
    return df


def reshape_df_wide_to_long(df):
    """Convert wide data to long. Finance team only needs rollups by year."""
    logging.debug("Melting df...")
    # Drop month/quarter columns, only keep year rollup
    start_year = int(os.getenv("START_YEAR"))
    end_year = int(os.getenv("END_YEAR"))
    year_strings = [str(x) for x in range(start_year, end_year + 1)]
    id_vars = ["AccountName", "AccountCode", "LevelName"]
    all_columns = id_vars + year_strings
    df = df[all_columns]
    # Pivot wide year columns to long - one row per year
    df = pd.melt(df, id_vars=id_vars, var_name="Year", value_name="Value")
    return df


def clean_and_filter_data(sql, df):
    """Clean up data types and filter out unwanted data"""
    levels_df = sql.query("SELECT LevelName FROM custom.Adaptive_Levels WHERE Export=1")
    levels = levels_df.LevelName.tolist()
    df = df.loc[df["LevelName"].isin(levels)].copy()
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    df.dropna(inplace=True)
    return df


def parse_data_export(sql, parsed_dict):
    """Convert parsed data to dataframe."""
    df = extract_data_from_parsed_dict(parsed_dict)
    df.rename(columns={col: col.replace(" ", "") for col in df.columns}, inplace=True)
    df = reshape_df_wide_to_long(df)
    df = clean_and_filter_data(sql, df)
    logging.info(f"Retrieved {len(df)} filtered and reshaped records.")
    return df


def main():
    adaptive = Adaptive()
    sql = MSSQL()
    xml = adaptive.export_data()
    parsed_dict = xmltodict.parse(xml)
    df = parse_data_export(sql, parsed_dict)
    sql.insert_into("Adaptive_Data", df, if_exists="replace", chunksize=10000)
    logging.info(f"Inserted {len(df)} records to Adaptive_Data.")


if __name__ == "__main__":
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    Mailer("Adaptive Connector").notify(error_message=error_message)
