from dataclasses import dataclass
from typing import Optional, List

from paramiko import Transport, SFTPClient, SFTPError
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


    def upload_csv(self, df: pd.DataFrame) -> None:

        ## Create connection
        conn = self._create_connection()

        ## Upload csv
        with conn.open(self.remote_path, "w") as file:
            file.write(df.to_csv(index = False))

        return None