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
@click.option('--verbose', '-v', is_flag=True, default=False, help="Print more output.")
def main(verbose):
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
    logFilesRecent, logFilesAll = [], []
    for logFile in logFiles:
        try:
            fileTime = datetime.strptime(logFile[-23:].replace('.csv', ''), "%Y-%m-%dT%H-%M-%S")
            if fileTime > datetime.now() - timedelta(hours=24):
                logFilesRecent.append(logFile)
        except ValueError:
            continue
    if verbose:
        print('Recent log files:')
        print(logFilesRecent)
    for logFile in logFilesRecent:
        download_file_share(file_share, logFile, f'export-data/{logFile}', sas_token)

    logsNames = [
        'completed-flow-logs',
        'drop-flow-logs',
        'completed-requests-for-help'
    ]

    for logName in logsNames:

        # Merge all recent logs
        dfLogsRecent = pd.DataFrame()
        for logFile in [file for file in logFilesRecent if logName in file]:
            df = pd.read_csv(logFile)
            if dfLogsRecent.empty:
                dfLogsRecent = df.copy()
            else:
                pd.concat([dfLogsRecent, df]).drop_duplicates().reset_index(drop=True)
        if verbose:
            print(f'Recent records of {logName}:')
            print(dfLogsRecent.tail())

        # Merge recent logs with master log file
        download_blob(blob_container, f"{logName}.csv", f"{logName}.csv")
        dfMasterLogs = pd.read_csv(f"{logName}.csv")
        if verbose:
            print(f'Master version of {logName}:')
            print(dfMasterLogs.tail())
        dfMasterLogs = pd.concat([dfMasterLogs, dfLogsRecent]).drop_duplicates().reset_index(drop=True)
        dfMasterLogs.to_csv(f"{logName}.csv", index=False)
        if verbose:
            print(f'New master version of {logName}:')
            print(dfMasterLogs.tail())

        # Upload master log file
        upload_blob(blob_container, f"{logName}.csv", f"{logName}.csv")

        logFilesAll.append(f"{logName}.csv")
        
    # Remove all logs
    logFilesAll += logFilesRecent
    for logFile in logFilesAll:
        os.remove(logFile)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)


if __name__ == "__main__":
    main()

