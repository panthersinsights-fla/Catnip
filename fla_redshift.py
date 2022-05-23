from dataclasses import dataclass, field
from typing import List

import pandas as pd
import pandas_redshift as pr

import os
from dotenv import load_dotenv

import pendulum

from fla_helpers import FLA_Helpers

@dataclass
class FLA_Redshift:

    load_dotenv(f"C:/Users/{os.getlogin()}/Florida Panthers/SP-BS - Documents/Data Science/Projects/Environment/.env")

    ## Database Info
    dbname: str = os.environ.get("KORE_REDSHIFT_DB_NAME")
    host: str = os.environ.get("KORE_REDSHIFT_HOST")
    port: int = os.environ.get("KORE_REDSHIFT_PORT")
    user: str = os.environ.get("KORE_REDSHIFT_USER_NAME")
    password: str = os.environ.get("KORE_REDSHIFT_PASSWORD")

    ## S3 Bucket Info
    aws_access_key_id: str = os.environ.get("FLA_S3_AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = os.environ.get("FLA_S3_AWS_SECRET_ACCESS_KEY")
    bucket: str = os.environ.get("FLA_S3_BUCKET_NAME")
    subdirectory: str = os.environ.get("FLA_S3_BUCKET_SUBDIRECTORY")

    def __post_init__(self):

        pr.connect_to_redshift(
            dbname = self.dbname,
            host = self.host,
            port = self.port,
            user = self.user,
            password = self.password,
        )

        pr.connect_to_s3(
            aws_access_key_id = self.aws_access_key_id,
            aws_secret_access_key = self.aws_secret_access_key,
            bucket = self.bucket,
            subdirectory = self.subdirectory,
        )   


    def write_to_warehouse(
            self,
            df: pd.DataFrame,
            table_name: str,
            append: bool = False,
            column_data_types: List = None
        ) -> None:

        df = self.create_processed_date(df)
        table_name_string = "custom." + table_name

        if append:
            pr.pandas_to_redshift(data_frame = df, redshift_table_name = table_name_string, column_data_types = column_data_types, append = True)
        else:
            pr.pandas_to_redshift(data_frame = df, redshift_table_name = table_name_string, column_data_types = column_data_types)

        pr.close_up_shop()


    def query_warehouse(self, sql_string = None, filepath = None) -> pd.DataFrame:

        '''
            Must input either:
                -> a string that is your sql statement
                -> a pathlib.Path object to a text file where your sql statement resides (for local testing)
        '''

        if filepath is not None:
            sql_string = FLA_Helpers().read_text_file(filepath)

        df = pr.redshift_to_pandas(sql_string)
        pr.close_up_shop()

        return df


    def create_processed_date(self, df : pd.DataFrame) -> pd.DataFrame:
        
        df['processed_date'] = pd.to_datetime(pendulum.now().isoformat())

        return df 