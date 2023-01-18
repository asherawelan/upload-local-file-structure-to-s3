# -*- coding: utf-8 -*-

from dotenv import load_dotenv
from alive_progress import alive_bar
from pprint import pprint
import os
import shutil
import sys
import boto3
import botocore
import atexit
import time
import correct_file_name
import hashlib

load_dotenv()

source_dir = os.getenv("SOURCE_DIR")
bucket = os.getenv("BUCKET")

s3 = boto3.client("s3",
                  aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                  region_name=os.getenv("REGION_NAME")
                  )


class NothingToDoException(Exception):
    """ Nothing to do """

class WrongTypeOfFileException(Exception):
    """ Wrong type of file """
def main():
    try:
        all_files_count = sum([len(files) for r, d, files in os.walk(source_dir)])

        with alive_bar(all_files_count) as bar:
            # Iterate over every line of the CSV
            for root, dirs, files in os.walk(source_dir):
                for file in files:
                    try:
                        if not file.endswith(('.pdf','.csv')):
                            raise WrongTypeOfFileException

                        local_file = os.path.join(root, file)
                        remote_file = local_file[8:]

                        print('Processing: ' + local_file)

                        local_checksum = get_local_checksum(local_file)
                        remote_checksum = get_remote_checksum(remote_file)

                        if remote_checksum == local_checksum:
                            raise NothingToDoException

                        update_remote_file(local_file, remote_file)

                    except NothingToDoException:
                        print('Checksum matches - nothing to do')
                        pass

                    except WrongTypeOfFileException:
                        pass

                    except botocore.exceptions.ClientError as e:
                        if e.response['Error']['Code'] in ['404', 'NoSuchKey']:
                            if 'Key' in e.response['Error']:
                                if 'pdf' in e.response['Error']['Key']:
                                    print((e.response['Error']))
                            continue
                        else:
                            print(e.response['Error']['Code'])
                    finally:
                        bar()

    except KeyboardInterrupt:
        pass

def update_remote_file(local_file, remote_file):
    print('Uploading: ' + remote_file)
    s3.upload_file(local_file, bucket, remote_file)

def get_remote_checksum(file):
    header = s3.head_object(Bucket=bucket, Key=file)

    return header['ETag'].strip('"')

def get_local_checksum(file):
    hash = hashlib.md5()
    with open(file, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash.update(chunk)

    return hash.hexdigest()

def create_correct_file_name(local_path, file):

    # Files created locally needed to be renamed to match the remote names.
    # This function finds what the name of the file is on the remote
    # to filter the included tuple.

    part_2, part_3 = [int(x) for x in local_path[8:].split('/')]
    part_1 = list(filter(lambda x: part_2 in x and part_3 in x, correct_file_name.data))[0][0]

    return os.path.join(
        str(part_3),
        str(part_2),
        str(part_1),
    ) + file[-4:]

@atexit.register
def exit_handler():
    print("Exiting")

if __name__ == "__main__":
    main()
