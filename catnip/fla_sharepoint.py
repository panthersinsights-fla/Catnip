from dataclasses import dataclass, field 

from prefect.client import Secret 

from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential

import pandas as pd

import os
import tempfile

from catnip.fla_helpers import FLA_Helpers

@dataclass
class Fla_Sharepoint:

    ## User information
    username : str = Secret("PI_MICROSOFT_USERNAME_EMAIL").get()
    password : str = Secret("PI_MICROSOFT_PASSWORD").get()

    ## Sharepoint paths
    base_url : str = "https://floridapanthers.sharepoint.com"
    site_path : str = "/sites/SP-BS/"
    project_folder : str = "Shared Documents/Data Science/Projects/"
    site_url : str = field(init = False)


    def __post_init__(self):
        
        ## Set paths
        self.site_url = self.base_url + self.site_path

        ## Authorize
        self.my_credentials = UserCredential(self.username, self.password)
        self.my_ctx = ClientContext(self.site_url).with_credentials(self.my_credentials)


    def upload_csv(
            self,
            df : pd.DataFrame,
            file_name : str = "thizz-iz-a-test",
            folder_path : str = "Testing-Folder",           # ie) Project/RestOfFolderPath
            add_log_date : bool = False
        ) -> None:
        
        file = FLA_Helpers().convert_df_to_csv_file_object(df)

        this_folder = self.connect_folder(folder_path)

        if add_log_date:
            target_file = this_folder.upload_file(file_name + FLA_Helpers().get_today_date_string_for_logging() + ".csv", file).execute_query()
        else:
            target_file = this_folder.upload_file(file_name + ".csv", file).execute_query()

        print(target_file.serverRelativeUrl)

        return None


    def upload_xml(
            self,
            df : pd.DataFrame,
            file_name : str = "thizz-iz-a-test",
            folder_path : str = "Testing-Folder",           # ie) Project/RestOfFolderPath
            add_log_date : bool = False
        ) -> None:
        
        file = FLA_Helpers().convert_df_to_xml_file_object(df)

        this_folder = self.connect_folder(folder_path)

        if add_log_date:
            target_file = this_folder.upload_file(file_name + FLA_Helpers().get_today_date_string_for_logging() + ".xml", file).execute_query()
        else:
            target_file = this_folder.upload_file(file_name + ".xml", file).execute_query()

        print(target_file.serverRelativeUrl)

        return None


    def download_file(
            self, 
            folder_path : str,
            file_name : str,
            is_csv : bool = False
        ):

        file_url = self.site_path + self.project_folder + folder_path + "/" + file_name

        download_path = os.path.join(tempfile.mkdtemp(), os.path.basename(file_url))

        with open(download_path, "wb") as local_file:
            self.my_ctx.web.get_file_by_server_relative_url(file_url).download(local_file).execute_query()

        if is_csv:
            file = pd.read_csv(download_path)
        else:
            file = FLA_Helpers().read_text_file(download_path)

        print("[Ok] file has been downloaded into: {0}".format(download_path))
        
        return file

    def connect_folder(self, folder_name : str = None):

        '''
            Project + rest of folder path
            ie) Testing-Folder/Logs
        '''
        
        return self.my_ctx.web.get_folder_by_server_relative_path(self.project_folder + folder_name)