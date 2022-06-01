from dataclasses import dataclass
from typing import List

from prefect.client import Secret 

import pandas as pd
import pandas_redshift as pr

import pendulum

from catnip.fla_helpers import FLA_Helpers

@dataclass
class FLA_Redshift:

    ## Database Info
    dbname: str = Secret("KORE_REDSHIFT_DB_NAME").get()
    host: str = Secret("KORE_REDSHIFT_HOST").get()
    port: int = Secret("KORE_REDSHIFT_PORT").get()
    user: str = Secret("KORE_REDSHIFT_USER_NAME").get()
    password: str = Secret("KORE_REDSHIFT_PASSWORD").get()

    ## S3 Bucket Info
    aws_access_key_id: str = Secret("FLA_S3_AWS_ACCESS_KEY_ID").get()
    aws_secret_access_key: str = Secret("FLA_S3_AWS_SECRET_ACCESS_KEY").get()
    bucket: str = Secret("FLA_S3_BUCKET_NAME").get()
    subdirectory: str = Secret("FLA_S3_BUCKET_SUBDIRECTORY").get()

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