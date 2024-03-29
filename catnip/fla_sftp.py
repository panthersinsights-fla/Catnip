from dataclasses import dataclass
from typing import Optional, List

from paramiko import Transport, SFTPClient, SFTPError
from os import getcwd

import pandas as pd


@dataclass
class FLA_Sftp:

    host: str
    username: str
    password: str
    remote_path: str

    port: int = 22


    def _create_connection(self) -> Optional[SFTPClient]:

        ## Establish transport object
        transport = Transport((self.host, self.port))
        transport.connect(username = self.username, password = self.password)

        ## Create SFTP connection object
        connection = SFTPClient.from_transport(transport)        

        return connection


    def get_all_filenames_in_directory(self) -> List:

        ## Create connection
        conn = self._create_connection()

        ## Get filenames
        filenames = conn.listdir(self.remote_path)
        
        return filenames


    def file_exists(self, conn: SFTPClient) -> bool:

        ## Check file existence
        try:
            conn.stat(self.remote_path)
            return True

        ## Else return error message
        except SFTPError as e:
            print(f"The specified file on this '{self.remote_path}' remote_path does not exist.")
            return False


    def download_csv(self, separator: str = ",") -> pd.DataFrame:

        ## Create connection
        conn = self._create_connection()

        ## Initialize dataframe
        df = pd.DataFrame()

        ## Download csv as dataframe
        if self.file_exists(conn):
            with conn.open(self.remote_path) as file:
                file.prefetch()
                df = pd.read_csv(file, sep = separator)

        return df


    def download_file(self, temp_filename: str) -> str:

        ## Create connection
        conn = self._create_connection()

        ## Create local path string
        local_path = f"{getcwd()}/{temp_filename}.{self.remote_path.split('.')[1]}"

        ## Download file and write to local path
        conn.get(self.remote_path, local_path)

        ## Notify path
        print(f"Moved {self.remote_path} to {local_path}")

        ## Return written path string
        return local_path
    

    def upload_csv(self, df: pd.DataFrame) -> None:

        ## Create connection
        conn = self._create_connection()

        ## Upload csv
        with conn.open(self.remote_path, "w") as file:
            file.write(df.to_csv(index = False))

        return None