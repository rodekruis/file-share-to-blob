import pandas as pd
from dateutil import parser
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.storage.fileshare import ShareDirectoryClient, ShareFileClient, generate_account_sas, ResourceTypes, AccountSasPermissions
import click
import logging
logging.root.handlers = []
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG, filename='ex.log')
# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.WARNING)
# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)
load_dotenv(dotenv_path="../credentials/.env")


def upload_blob(container, file_path, blob_path):
    blob_client = BlobServiceClient.from_connection_string(os.getenv("ACCOUNT_CONNECTION_STRING")).\
        get_blob_client(container=container, blob=blob_path)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)


def download_blob(container, file_path, blob_path):
    blob_client = BlobServiceClient.from_connection_string(os.getenv("ACCOUNT_CONNECTION_STRING")).\
        get_blob_client(container=container, blob=blob_path)
    with open(file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())


def list_blobs(container):
    container_client = BlobServiceClient.from_connection_string(os.getenv("ACCOUNT_CONNECTION_STRING")).\
        get_container_client(container)
    blob_list = container_client.list_blobs()
    return [f"{blob.name}" for blob in blob_list]


def upload_file_share(share, file_path, share_path, sas_token):
    file_client = ShareFileClient(
        account_url=f"https://{os.getenv('ACCOUNT_NAME')}.file.core.windows.net",
        credential=sas_token,
        share_name=share,
        file_path=share_path)
    with open(file_path, "rb") as source_file:
        file_client.upload_file(source_file)


def download_file_share(share, file_path, share_path, sas_token):
    file_client = ShareFileClient(
        account_url=f"https://{os.getenv('ACCOUNT_NAME')}.file.core.windows.net",
        credential=sas_token,
        share_name=share,
        file_path=share_path)
    with open(file_path, "wb") as file_handle:
        data = file_client.download_file()
        data.readinto(file_handle)


def list_file_share(share, directory, sas_token):
    parent_dir = ShareDirectoryClient(
        account_url=f"https://{os.getenv('ACCOUNT_NAME')}.file.core.windows.net",
        credential=sas_token,
        share_name=share,
        directory_path=directory)
    share_list = list(parent_dir.list_directories_and_files())
    return [f"{share.name}" for share in share_list]


@click.command()
def main():
    utc_timestamp = datetime.utcnow()

    sas_token = generate_account_sas(
        account_name=os.getenv("ACCOUNT_NAME"),
        account_key=os.getenv("ACCOUNT_KEY"),
        resource_types=ResourceTypes(service=True, container=True, object=True),
        permission=AccountSasPermissions(
            read=True, write=True, delete=True, list=True, add=True, create=True, update=True, process=True,
            delete_previous_version=True),
        expiry=utc_timestamp + timedelta(hours=1)
    )
    file_share = os.getenv("FILE_SHARE")
    blob_container = os.getenv("BLOB_CONTAINER")

    # Get recent logs
    logFiles = list_file_share(file_share, 'export-data', sas_token)
    logFilesRecent = []
    for logFile in logFiles:
        fileTime = datetime.strptime(logFile[-23:].replace('.csv', ''), "%Y-%m-%dT%H-%M-%S")
        if fileTime > datetime.now() - timedelta(hours=24):
            logFilesRecent.append(logFile)
    for logFile in logFilesRecent:
        download_file_share(file_share, logFile, f'export-data/{logFile}', sas_token)

    logsNames = [
        'completed-flow-logs',
        'drop-flow-logs',
        'completed-requests-for-help'
    ]

    for logName in logsNames:

        # Merge all recent logs
        dfRecentLogs = pd.DataFrame()
        for logFile in [file for file in logFilesRecent if logName in file]:
            df = pd.read_csv(logFile)
            if dfRecentLogs.empty:
                dfRecentLogs = df.copy()
            else:
                pd.concat([dfRecentLogs, df]).drop_duplicates().reset_index(drop=True)

        # Merge recent logs with master log file
        download_blob(blob_container, f"{logsNames}.csv", f"{logsNames}.csv")
        dfMasterLogs = pd.read_csv(f"{logsNames}.csv")
        pd.concat([dfMasterLogs, dfRecentLogs]).drop_duplicates().reset_index(drop=True)

        # Upload master log file
        upload_blob(blob_container, f"{logsNames}.csv", f"{logsNames}.csv")

    logging.info('Python timer trigger function ran at %s', utc_timestamp)


if __name__ == "__main__":
    main()

