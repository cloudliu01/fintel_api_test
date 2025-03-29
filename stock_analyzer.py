import os
import json
import pandas as pd
import logging
from common import OutputPathSingleton

DEBUG = os.environ.get("DEBUG", False)

class StockOwnershipAnalyzer:
    """
    Loads ownership data from saved JSON files and analyzes the top 10 biggest holders.
    """

    def __init__(self, data_dir, slug_list=[], mode='stock'):   
        """
        Initializes the analyzer with the directory containing JSON files and a list of slugs.
        :param data_dir: Directory where JSON files are stored.
        :param slug_list: List of stock slugs to analyze.
        :param mode: Mode of operation, either 'stock' or 'fund'.
        """
        self.data_dir = data_dir
        self.slug_list = slug_list 
        self.mode = mode 
        self.data = []
        self.df = None

    def load_data(self):
        """
        Loads all JSON files in the specified directory and parses them into a DataFrame.
        """
        if not os.path.exists(self.data_dir):
            logging.error(f"Directory does not exist: {self.data_dir}")
            return False

        all_data = []
        
        specified_json_files = [f"{slug}.json" for slug in self.slug_list]
        for filename in os.listdir(self.data_dir):
            if filename.endswith(".json") and filename in specified_json_files:
                file_path = os.path.join(self.data_dir, filename)

                try:
                    with open(file_path, "r") as f:
                        data = json.load(f)
                        all_data.append(data)
                except Exception as e:
                    logging.error(f"Failed to load {filename}: {e}")
        
        if not all_data:
            logging.warning("No valid data files loaded.")
            return False

        self.data = all_data
        self.df = self._parse_data()
        return True

    def _parse_data(self):
        """
        Parses JSON data into a structured Pandas DataFrame.
        """
        df_list = []
        if self.mode == 'stock':
            for data in self.data:
                owners_data = data.get("owners", [])

                if owners_data:
                    temp_df = pd.json_normalize(owners_data)
                    temp_df["symbol"] = data.get("symbol", "Unknown")
                    temp_df["exchange"] = data.get("exchange", "Unknown")  # Add exchange info
                    temp_df['country'] = data.get("country", "Unknown")  # Add country info
                    temp_df['company_name'] = data.get("name", "Unknown")  # Add country info
                    temp_df['company_url'] = data.get("url", "Unknown")  # Add country info
                    df_list.append(temp_df)
        else:
            for data in self.data:
                temp_df = self.flatten_holdings_data(data)
                df_list.append(temp_df)
        return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()




    def flatten_holdings_data(self, raw_data):
        owner = raw_data.get("owner", {})
        holdings = raw_data.get("holdings", [])

        flat_rows = []

        for h in holdings:
            flat_row = self.flatten_record(owner, h)

            flat_rows.append(flat_row)

        return pd.DataFrame(flat_rows)


    def flatten_record(self, owner_data, holding_data):
        """
        Flatten one 'holding' record plus its nested 'security',
        merging in 'owner' fields. For any repeated key like 'id'
        we prefix it with the parent (owner_id, holdings_id, security_id).
        """
        row = {}

        # --- 1) Bring in owner keys ---
        for ok, ov in owner_data.items():
            # If this key also appears in holding or security, rename it
            if ok in ("id", "name", "exchangeId", "symbol"):
                row[f"owner_{ok}"] = ov
            else:
                row[ok] = ov

        # --- 2) Bring in holding keys ---
        # We’ll first copy everything except 'security'
        # then flatten 'security' separately
        security_data = holding_data.get("security", {}) if holding_data.get('security') else {}
        holding_copy = dict(holding_data)
        holding_copy.pop("security", None)

        for hk, hv in holding_copy.items():
            if hk in ("id", "name", "exchangeId", "symbol"):
                row[f"holdings_{hk}"] = hv
            else:
                row[hk] = hv

        # --- 3) Flatten the security sub-dict ---
        for sk, sv in security_data.items():
            if sk in ("id", "name", "exchangeId", "symbol"):
                row[f"security_{sk}"] = sv
            else:
                row[sk] = sv

        return row