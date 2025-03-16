import pandas as pd
import json
import requests
import pytz
import os
from termcolor import colored
from datetime import datetime
from warnings import warn   


# 美股7姐妹 
SISTER_7_SLUGS = ['googl', 'amzn', 'meta', 'nvda', 'aapl', 'msft', 'tsla']
retrieved_json_path = "results/retrieved_json"

DEBUG = True  # 1, force retrieve

X_API_KEY = os.environ.get("X-API-KEY")


def parse_to_dataframe(data):
    """
    Parses ownership data into a pandas DataFrame, dynamically handling arbitrary fields.
    
    :param data: Dictionary containing ownership information.
    :return: Pandas DataFrame.
    """
    try:
        # Extract the owners list
        owners_data = data.get("owners", [])
    
        if not owners_data:
            return pd.DataFrame()  # Return empty DataFrame if no owners exist
    
        # Convert owners data into DataFrame using pandas built-in function
        df = pd.json_normalize(owners_data)

        # Add symbol, exchange, country, name, and url as additional columns
        df.insert(0, "symbol", data.get("symbol"))
        df.insert(1, "exchange", data.get("exchange"))
        df.insert(2, "country", data.get("country"))
        df.insert(3, "company_name", data.get("name"))
        df.insert(4, "company_url", data.get("url"))

        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()


def retrieve_ownership_data(slug):
    """
    Retrieves ownership data for a given stock symbol.
    
    :param slug: Stock symbol.
    :return: Ownership data.
    """
    try:
        url = f"https://api.fintel.io/data/v/0.0/so/us/{slug}"
        headers = {
            "accept": "application/json",
            "X-API-KEY": X_API_KEY
        }
        response = requests.get(url, headers=headers)
        return response.json()
    except Exception as e:
        print(f"Error: {e}")
        return {}


# define a funtion to retrieve data for 7 sisters and save to results folder
def retrieve_7_sisters_data():
    try:
        results_dir = get_output_file_path()
        if not results_dir:
            return False
        for slug in SISTER_7_SLUGS:
            file_path = os.path.join(results_dir, f"{slug}.json")

            if os.path.exists(file_path):
                print(f"Skipping {slug}, file already exists.")
                continue

            data = retrieve_ownership_data(slug)
            with open(file_path, "w") as f:
                json.dump(data, f)
                print(f"Saved {file_path}")
    
        print("Done")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False


# get output file path
def get_output_file_path() -> str:
    """
    Gets the output file path in the format: results/retrieved_json/20250316_AM (or PM/NIGHT),
    skipping weekends (Saturday=5, Sunday=6).
    
    Returns:
        str: The directory path if not a weekend, otherwise None.
    """
    try:
        # 1) 获取美国东部时区的当前时间
        eastern_tz = pytz.timezone("US/Eastern")
        now_est = datetime.now(eastern_tz)
        # Skip weekends (Sat=5, Sun=6)
        if not DEBUG and now_est.weekday() in (5, 6):
            # show warning message with yellow text
            warn(colored("It's the weekend, skipping data retrieval.", "yellow"))
            return None

        # 2) 根据小时判断是 AM, PM, 还是 NIGHT
        hour = now_est.hour
        if 6 <= hour < 12:
            suffix = "AM"
        elif 12 <= hour < 18:
            suffix = "PM"
        else:
            suffix = "NIGHT"

        # 3) 以 YYYYMMDD_XX（AM/PM/NIGHT） 的形式创建目录
        date_str = now_est.strftime("%Y%m%d")
        results_dir = os.path.join(retrieved_json_path, f"{date_str}_{suffix}")
        os.makedirs(results_dir, exist_ok=True)
        return results_dir
    except Exception as e:
        print(f"Error: {e}")
        return None



if __name__ == "__main__":
    retrieve_7_sisters_data()


## Load json from results/tsla.json
#with open("results/tsla.json", "r") as f:
#    data = json.load(f)
#    df = parse_to_dataframe(data)
#    # sort by ['ownership_percent'] in descending order 
#    df_1 = df.sort_values(by='ownership_percent', ascending=False)
#    df_1.to_csv("results/tsla_owners.csv", sep='\t', index=False)
#    print('Done')

