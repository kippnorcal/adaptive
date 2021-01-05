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


def remove_spaces_from_column_names(df):
    """Remove spaces from dataframe column names and replace with empty string."""
    df.rename(columns={col: col.replace(" ", "") for col in df.columns}, inplace=True)


def reshape_df_wide_to_long(df):
    """Convert wide data to long. Finance team only needs rollups by year."""
    logging.debug("Melting df...")
    # Drop month/quarter columns, only keep year rollup
    start_year = int(os.getenv("ACCOUNTS_START"))
    end_year = int(os.getenv("ACCOUNTS_END"))
    year_strings = [str(x) for x in range(start_year, end_year + 1)]
    id_vars = ["AccountName", "AccountCode", "LevelName"]
    all_columns = id_vars + year_strings
    df = df[all_columns]
    # Pivot wide year columns to long - one row per year
    df = pd.melt(df, id_vars=id_vars, var_name="Year", value_name="Value")
    return df


def clean_and_filter_account_data(sql, df, levels):
    """Clean up data types and filter out unwanted data"""
    df = df.loc[df["LevelName"].isin(levels)].copy()
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    df.dropna(inplace=True)
    return df


def parse_account_data_export(sql, xml, levels):
    """Parse account data embedded in xml to dataframe."""
    parsed_dict = xmltodict.parse(xml)
    data = parsed_dict["response"]["output"]
    data.lstrip("![CDATA[").rstrip("]]")
    # StringIO was failing (possibly due to the size of the data)
    with open("data.csv", "w") as f:
        f.write(data)
    df = pd.read_csv("data.csv")
    remove_spaces_from_column_names(df)
    df = reshape_df_wide_to_long(df)
    df = clean_and_filter_account_data(sql, df, levels)
    logging.debug(f"Retrieved {len(df)} filtered and reshaped records.")
    return df


def parse_personnel_export(sql, xml):
    """Parse personnel data embedded in xml to dataframe."""
    parsed_dict = xmltodict.parse(xml)
    if parsed_dict.get("response").get(
        "output"
    ):  # output is none for some levels with no personnel
        data = parsed_dict["response"]["output"]["data"]["#text"]
        df = pd.read_csv(StringIO(data))
        # drop compensation columns per finance request for now
        df.drop(
            columns=[
                "Base Sal",
                "NS Base Sal",
                "Sal Adj",
                "Tot Eff Sal",
                "Hrly Rate",
                "Tenure Bon",
                "OT Pay",
                "Flex Bon",
                "Tot Comp",
            ],
            inplace=True,
        )
        remove_spaces_from_column_names(df)
        logging.debug(f"Retrieved {len(df)} filtered and reshaped records.")
        return df
    else:
        return None


def sync_accounts_data(sql, adaptive):
    """Export and parse account data"""
    levels = sql.query(
        "SELECT LevelName FROM custom.Adaptive_Levels WHERE ExportData=1"
    )
    levels = levels.LevelName.tolist()
    account_data = adaptive.export_data()
    df = parse_account_data_export(sql, account_data, levels)
    sql.insert_into("Adaptive_AccountValues", df, if_exists="replace", chunksize=10000)
    logging.info(f"Inserted {len(df)} records to Adaptive_AccountValues.")


def sync_personnel_data(sql, adaptive):
    """Export and parse personnel data"""
    levels = sql.query(
        "SELECT LevelName FROM custom.Adaptive_Levels WHERE ExportPersonnel=1"
    )
    levels = levels.LevelName.tolist()
    all_dfs = []
    for level in levels:
        xml = adaptive.export_configurable_model_data(level)
        df_by_level = parse_personnel_export(sql, xml)
        if df_by_level is not None:
            all_dfs.append(df_by_level)
    df = pd.concat(all_dfs)
    sql.insert_into("Adaptive_Personnel", df, if_exists="replace")
    logging.info(f"Inserted {len(df)} records to Adaptive_Personnel.")


def main():
    adaptive = Adaptive()
    sql = MSSQL()
    sync_accounts_data(sql, adaptive)
    sync_personnel_data(sql, adaptive)


if __name__ == "__main__":
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    Mailer("Adaptive Connector").notify(error_message=error_message)
