import logging
import boto3

from ols.ofl_commons.infrastructure.FileRepo.file_repo import FileRepo


class S3FileRepo(FileRepo):
    def __init__(self, config):
        self._aws_access_key_id = config["S3"]['aws_access_key_id']
        self._aws_secret_access_key = config["S3"]['aws_secret_access_key']
        self._region_name = config["S3"]['region']
        self._endpoint_url = config["S3"]['endpoint_url']
        self._bucket_id = config["S3"]['bucket_id']
        self.client = boto3.client(
            service_name = 's3',
            aws_access_key_id = self._aws_access_key_id,
            aws_secret_access_key = self._aws_secret_access_key,
            region_name = self._region_name,
            endpoint_url = self._endpoint_url,
        )
        logging.debug(f"S3 file repo config is: \n"
                      f"aws_access_key_id: {self._aws_access_key_id}\n"
                      f"aws_secret_access_key: {self._aws_secret_access_key}\n"
                      f"region_name: {self._region_name}\n"
                      f"endpoint_url: {self._endpoint_url}\n"
                      f"bucket_id: {self._bucket_id}\n"
                      )

    def download_file(self, local_path: str, target_path: str) -> bool:
        try:
            self.client.download_file(self._bucket_id, target_path, local_path)
            return True
        except Exception as e:
            logging.error(f"S3 download file failed!")
            logging.exception(e)
            return False

    def upload_file(self, local_path: str, target_path: str) -> bool:
        try:
            self.client.upload_file(local_path, self._bucket_id, target_path)
            return True
        except Exception as e:
            logging.error(f"S3 upload file failed!")
            logging.exception(e)
            return False

    def delete_file(self, target_path: str) -> bool:
        try:
            self.client.delete_object(Bucket=self._bucket_id, Key=target_path)
            return True
        except Exception as e:
            logging.error(f"S3 delete file failed!")
            logging.exception(e)
            return False

    def download_payload(self, local_path: str, target_path: str) -> bool:
        try:
            self.download_file(local_path=local_path, target_path=target_path)
            self.delete_file(target_path=target_path)
            return True
        except Exception as e:
            logging.error(f"S3 download payload file failed!")
            logging.exception(e)
            return False
