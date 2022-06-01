from dataclasses import dataclass, field

from prefect.client import Secret 

import pandas as pd
import pyodbc 

import os

from fla_helpers import FLA_Helpers


@dataclass
class FLA_Archtics:

    data_source_name : str = Secret("TM_DB_DSN").get() #os.environ.get("TM_DB_DSN")
    user_id : str = Secret("TM_DB_USER_ID").get() #os.environ.get("TM_DB_USER_ID")
    password : str = Secret("TM_DB_PASSWORD").get() #os.environ.get("TM_DB_PASSWORD")

    driver : str = "{SQL Anywhere 12}"
    host : str = Secret("TM_DB_HOST").get() #os.environ.get("TM_DB_HOST")
    database_name : str = Secret("TM_DB_NAME").get() #os.environ.get("TM_DB_DB_NAME")
    port : str = Secret("TM_DB_PORT").get() #os.environ.get("TM_DB_PORT")

    connection_string : str = field(init = False)

    def __post_init__(self):

        self.connection_string = repr(f'''
            Driver={self.driver};
            Host={self.host};
            DBN={self.database_name};
            UID={self.user_id};
            PWD={self.password}
        ''').strip().replace("\\n", "").replace("  ", "").replace("'", "")


    def query_database(self, sql_string = None, filepath = None):

        '''
            Must input either:
                -> a string that is your sql statement
                -> a pathlib.Path object to a text file where your sql statement resides (for local testing)
        '''
        
        if filepath is not None:
            sql_string = FLA_Helpers().read_text_file(filepath)

        # cnxn = pyodbc.connect(
        #     dsn = self.dsn,
        #     uid = self.uid,
        #     pwd = self.pwd
        #     )

        cnxn = pyodbc.connect(self.connection_string, readonly = True)

        return pd.read_sql(sql_string, cnxn) 
