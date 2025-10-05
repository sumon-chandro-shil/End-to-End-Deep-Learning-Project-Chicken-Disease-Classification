import os
import zipfile
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
from cnnClassifier import logger
from cnnClassifier.utils.common import get_size
from dotenv import load_dotenv
from cnnClassifier.entity.config_entity import DataIngestionConfig
from pathlib import Path


class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config
        self.drive = self._authenticate()

    def _authenticate(self):
        """Authenticate Google Drive using service account credentials."""
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.config.service_account_file, scopes=self.config.scopes
        )
        gauth = GoogleAuth()
        gauth.credentials = credentials
        return GoogleDrive(gauth)

    def download_folder(self, folder_id=None):
        """Download all files (recursively) from a Google Drive folder.
        Zip file will be saved directly as local_data_file."""
        
        if folder_id is None:
            folder_id = self.config.root_folder_id

        # Ensure root directory exists
        os.makedirs(self.config.root_dir, exist_ok=True)
        
        # Ensure local_data_file's parent directory exists
        os.makedirs(os.path.dirname(self.config.local_data_file), exist_ok=True)

        file_list = self.drive.ListFile(
            {'q': f"'{folder_id}' in parents and trashed=false"}
        ).GetList()

        for file in file_list:
            file_title = file['title']

            if file['mimeType'] == 'application/vnd.google-apps.folder':
                # ignore subfolders
                continue
            else:
                # Save zip directly to local_data_file
                print(f"Downloading {file_title}...")
                file.GetContentFile(self.config.local_data_file)
                print(f"{file_title} downloaded at {self.config.local_data_file}!")



    def extract_zip_file(self):
        """
        zip_file_path: str
        Extracts the zip file into the data directory
        Function return None
        """

        unzip_path = self.config.unzip_dir
        os.makedirs(unzip_path, exist_ok=True)
        with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
            zip_ref.extractall(unzip_path)


