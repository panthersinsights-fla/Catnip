from dataclasses import dataclass

import pandas as pd

from io import StringIO
from typing import List

from datetime import datetime
import pendulum

from prefect import context

@dataclass
class FLA_Helpers:
    
    @staticmethod
    def read_text_file(filepath : str) -> str:

        with open(filepath, 'r') as file:
            this_string = file.read()
        
        return this_string

    @staticmethod
    def convert_df_to_csv_file_object(df : pd.DataFrame):

        file = StringIO()
        df.to_csv(file)
        file.seek(0)

        return file

    @staticmethod
    def get_today_date_string_for_logging() -> str:
        return "-" + str(pendulum.now().to_datetime_string())[:-9] + "-" + str(pendulum.now().to_datetime_string()).replace(":", "")[-6:]

    @staticmethod
    def get_current_datetime() -> pd.Timestamp:
        return pd.to_datetime(pendulum.now().isoformat())

    @staticmethod
    def format_currency(x):
        return '${0:,.2f}'.format(x)

    @staticmethod
    def format_currency_no_cents(x):
        return '${0:,.0f}'.format(x)

    @staticmethod
    def safe_division(n, d):
        return n / d if d else 0

    @staticmethod
    def format_numbers_with_commas(x):
        return '{:,.0f}'.format(x)

    @staticmethod
    def format_with_percents(x) -> str:    
        return str(round((x*100), 1)) + "%"

    @staticmethod
    def fill_na_by_type(df : pd.DataFrame) -> pd.DataFrame:

        for col in df:
            dt = df[col].dtype 
            if dt == int or dt == float:
                df[col] = df[col].fillna(0)
            else:
                df[col] = df[col].fillna("Other")

        return df 

    @staticmethod
    def get_days_between(d1, d2):

        d1 = datetime.strptime(d1, "%Y-%m-%d")
        d2 = datetime.strptime(d2, "%Y-%m-%d")

        return abs((d2 - d1).days)


    @staticmethod
    def list_to_string_for_sql(this_is_a_list : List) -> str:

        return str(this_is_a_list).replace("[", "(").replace("]", ")")

    
    @staticmethod
    def sort_df_cols(df : pd.DataFrame) -> pd.DataFrame: 

        ## Sort Columns Alphabetically ##
        
        return df.reindex(sorted(df.columns), axis = 1)

    
    @staticmethod
    def write_to_logs(log_text : str) -> None:

        logger = context["logger"]
        logger.info(f"{log_text}")

        return None