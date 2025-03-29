import os
import logging
import pytz
import logging
import pandas as pd
from datetime import datetime, timedelta
from warnings import warn
from termcolor import colored

DEBUG = os.environ.get("DEBUG", False)
RETRIEVED_JSON_PATH = "results/retrieved_json"

class OutputPathSingleton:
    """
    Singleton class to ensure the same output directory is used throughout execution.
    """

    _instance = None
    _output_path = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(OutputPathSingleton, cls).__new__(cls)
            cls._output_path = cls._generate_output_path()
        return cls._instance

    @classmethod
    def _generate_output_path(cls):
        """
        Generates the output file path with format: results/retrieved_json/YYYYMMDD_AM/PM/NIGHT
        Avoids creating different paths when called multiple times.
        """
        try:
            eastern_tz = pytz.timezone("US/Eastern")
            now_est = datetime.now(eastern_tz)

            ## Skip weekends unless in debug mode
            #if not DEBUG and now_est.weekday() in (5, 6):
            #    warn(colored("It's the weekend, skipping data retrieval.", "yellow"))
            #    return None

            # Construct path
            date_str = now_est.strftime("%Y%m%d")
            results_dir = os.path.join(RETRIEVED_JSON_PATH, f"{date_str}")
            os.makedirs(results_dir, exist_ok=True)
            return results_dir
        except Exception as e:
            logging.error(f"Error generating output path: {e}")
            return None

    @classmethod
    def get_path(cls):
        """Returns the singleton output path."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance._output_path



def convert_timestamp_columns(df: pd.DataFrame, columns: list[str] = None, format_map: dict = { 'fileDate': '%Y-%m-%d', 'effectiveDate': '%Y-%m-%d', 'formattedFileDate': '%m-%d' }) -> pd.DataFrame:
    """
    Convert timestamp columns to datetime using optional format mapping.

    :param df: Input DataFrame
    :param columns: List of columns to convert. If None, auto-detect date columns.
    :param format_map: Optional dict like {'fileDate': '%Y-%m-%d', 'formattedFileDate': '%m-%d'}
    :return: DataFrame with converted datetime columns
    """
    df = df.copy()

    if columns is None:
        columns = [col for col in df.columns if "date" in col.lower() or "time" in col.lower()]

    for col in columns:
        fmt = format_map[col] if format_map and col in format_map else None
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce', format=fmt).replace({pd.NaT: None})
        except Exception as e:
            print(f"Could not convert column '{col}': {e}")

    return df


def top_shareholders_by_symbol(df, top_n=20):
    """
    For each symbol in the DataFrame, return the top N owners by total shares.

    :param df: pandas DataFrame with at least 'symbol', 'name', 'shares' columns
    :param top_n: number of top owners to return per symbol
    :return: dictionary of {symbol: top N owners DataFrame}
    """
    result = {}

    # Drop rows where symbol, name or shares are missing
    df = df.dropna(subset=["symbol", "name", "shares"])

    # Ensure 'shares' is numeric
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0)

    # Group by symbol
    for symbol, group in df.groupby("symbol"):
        top_owners = (
            group.groupby(["name", "slug"], as_index=False)["shares"]
            .sum()
            .sort_values(by="shares", ascending=False)
            .head(top_n)
        )
        result[symbol] = top_owners

    return result



def parse_formatted_file_date(value):
    today = datetime.today().date()

    if isinstance(value, str):
        value = value.strip()

        # Case 1: "X days ago"
        if "days ago" in value:
            try:
                days = int(value.split()[0])
                return today - timedelta(days=days)
            except ValueError:
                return pd.NaT

        # Case 2: "MM-DD" style
        try:
            month, day = map(int, value.split("-"))
            parsed_date = datetime(today.year, month, day).date()
            # If this date is in the future (i.e., hasn't occurred yet this year), assume it's from last year
            if parsed_date > today:
                parsed_date = datetime(today.year - 1, month, day).date()
            return parsed_date
        except Exception:
            return pd.NaT

    return pd.NaT
