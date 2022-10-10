from dataclasses import dataclass, field

from prefect.client import Secret 

import pandas as pd
import pyodbc 

from catnip.fla_helpers import FLA_Helpers


@dataclass
class FLA_Archtics:

    data_source_name : str = Secret("TM_DB_DSN").get()
    user_id : str = Secret("TM_DB_USER_ID").get()
    password : str = Secret("TM_DB_PASSWORD").get()

    driver : str = "{SQL Anywhere 12}"
    host : str = Secret("TM_DB_HOST").get()
    database_name : str = Secret("TM_DB_NAME").get()
    port : str = Secret("TM_DB_PORT").get()

    connection_string : str = field(init = False)

    def __post_init__(self):

        self.connection_string = repr(f'''
            Driver={self.driver};
            Host={self.host};
            DBN={self.database_name};
            UID={self.user_id};
            PWD={self.password}
        ''').strip().replace("\\n", "").replace("  ", "").replace("'", "")


    def query_database(self, sql_string = None, filepath = None) -> pd.DataFrame:

        '''
            Must input either:
                -> a string that is your sql statement
                -> a pathlib.Path object to a text file where your sql statement resides (for local testing)
        '''
        
        if filepath is not None:
            sql_string = FLA_Helpers().read_text_file(filepath)

        cnxn = pyodbc.connect(self.connection_string, readonly = True)

        return pd.read_sql(sql_string, cnxn) 
