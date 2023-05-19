from dataclasses import dataclass

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime

from io import StringIO
import re
from typing import List, Union

from datetime import datetime
import pendulum

from prefect import context

@dataclass
class FLA_Helpers:
    
    @staticmethod
    def read_text_file(filepath: str) -> str:

        with open(filepath, 'r') as file:
            this_string = file.read()
        
        return this_string

    @staticmethod
    def convert_df_to_csv_file_object(df: pd.DataFrame):

        file = StringIO()
        df.to_csv(file)
        file.seek(0)

        return file

    @staticmethod
    def convert_df_to_xml_file_object(df: pd.DataFrame):

        file = StringIO()
        df.to_xml(file)
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
    def fill_na_by_type(df: pd.DataFrame) -> pd.DataFrame:

        for col in df.columns:
            dt = df[col].dtype 
            if dt == int or dt == float:
                df[col] = df[col].fillna(0)
            elif is_datetime(dt):
                df[col] = df[col].fillna(datetime(2012, 12, 21))
            elif dt == object:
                df[col] = df[col].fillna("N/A")

        return df 

    @staticmethod
    def get_days_between(d1: Union[datetime, str], d2: Union[datetime, str]) -> int:

        if (type(d1) is not datetime) and (type(d2) is not datetime):
            try:
                d1 = datetime.strptime(d1, "%Y-%m-%d")
                d2 = datetime.strptime(d2, "%Y-%m-%d")
            except: 
                print("must be either a datetime object or a string in the format of YYYY-MM-DD !!")

        return abs((d2 - d1).days)


    @staticmethod
    def list_to_string_for_sql(this_is_a_list: List) -> str:

        return str(this_is_a_list).replace("[", "(").replace("]", ")")

    
    @staticmethod
    def sort_df_cols(df: pd.DataFrame) -> pd.DataFrame: 

        ## Sort Columns Alphabetically ##
        
        return df.reindex(sorted(df.columns), axis = 1)

    
    @staticmethod
    def write_to_logs(log_text: str) -> None:

        logger = context["logger"]
        logger.info(f"{log_text}")

        return None


    @staticmethod
    def unnest_json_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:

        unnest_df = pd.json_normalize(df[column_name])
        unnest_df.columns = [f"{s}_{column_name}".lower().strip().replace(" ", "_") for s in unnest_df.columns]
        
        df = df.join(unnest_df)
        df = df.drop(columns=[column_name])

        return df 


    @staticmethod
    def standardize_column_names(df: pd.DataFrame, snake_case: bool = False) -> pd.DataFrame:

        if snake_case:
            pattern = re.compile(r'(?<!^)(?=[A-Z])')
            df.columns = [pattern.sub('_', s).lower() for s in df.columns]
        
        df.columns = [str(s).replace(" ", "_").replace("/", "_").replace(".", "_") for s in df.columns] 
        df.columns = [''.join(e.lower() for e in s if e.isalnum() or e == "_") for s in df.columns] 

        return df 


    def pd_dtype_to_redshift_dtype(self, dtype: str) -> str:

        if dtype.startswith('int64'):
            return 'BIGINT'
        elif dtype.startswith('int'):
            return 'INTEGER'
        elif dtype.startswith('float'):
            return 'FLOAT'
        elif dtype.startswith('datetime'):
            return 'TIMESTAMP'
        elif dtype == 'bool':
            return 'BOOLEAN'
        else:
            return 'VARCHAR(MAX)'

    def get_column_data_types(self, df: pd.DataFrame) -> List:
        return [self.pd_dtype_to_redshift_dtype(str(dtype.name).lower()) for dtype in df.dtypes.values]

    @staticmethod
    def to_bool(s):
        return 1 if s else 0
