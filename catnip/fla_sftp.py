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

        transport = Transport((self.host, self.port))
        transport.connect(username = self.username, password = self.password)

        connection = SFTPClient.from_transport(transport)        

        return connection

    def get_all_filenames_in_directory(self) -> List:

        conn = self._create_connection()

        return conn.listdir(self.remote_path)

    def file_exists(self, conn: SFTPClient) -> bool:

        try:
            conn.stat(self.remote_path)
            return True

        except SFTPError as e:
            print(f"The specified file on this '{self.remote_path}' remote_path does not exist.")
            return False

    def download_csv(self) -> pd.DataFrame:

        conn = self._create_connection()

        df = pd.DataFrame()

        if self.file_exists(conn):
            with conn.open(self.remote_path) as file:
                file.prefetch()
                df = pd.read_csv(file)

        return df

    def upload_csv(self, df: pd.DataFrame) -> None:

        conn = self._create_connection()

        with conn.open(self.remote_path, "w") as file:
            file.write(df.to_csv(index = False))

        return None