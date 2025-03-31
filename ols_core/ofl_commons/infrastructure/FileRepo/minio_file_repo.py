from abc import ABC, abstractmethod
import yaml
from minio import Minio
import logging

from ols.ofl_commons.infrastructure.FileRepo.file_repo import FileRepo

# class FileRepo(ABC):
#     @abstractmethod
#     def upload_file(self, local_path: str, target_path: str) -> None:
#         pass
#
#     @abstractmethod
#     def download_file(self, local_path: str, target_path: str) -> None:
#         pass
#
#     @abstractmethod
#     def delete_file(self, target_path: str) -> None:
#         pass


class MinioFileRepo(FileRepo):
    def __init__(self, config):
        endpoint = config["minio"]["endpoint"]
        access_key = config["minio"]["accessKey"]
        secret_key = config["minio"]["secretKey"]
        self.bucket_name = config["minio"]["bucketName"]
        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

    def upload_file(self, local_path: str, target_path: str) -> bool:
        try:
            self.client.fput_object(self.bucket_name, target_path, local_path)
            return True
        except Exception as e:
            logging.error(f"S3 upload file failed!")
            logging.exception(e)
            return False

    def download_file(self, local_path: str, target_path: str) -> bool:
        try:
            self.client.fget_object(self.bucket_name, target_path, local_path)
            return True
        except Exception as e:
            logging.error(f"S3 upload file failed!")
            logging.exception(e)
            return False

    def delete_file(self, target_path: str) -> bool:
        try:
            self.client.remove_object(self.bucket_name, target_path)
            return True
        except Exception as e:
            logging.error(f"S3 upload file failed!")
            logging.exception(e)
            return False

    def listdir_object_name(self, prefix: str = ""):
        objects_minio = self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
        objects = []
        for obj in objects_minio:
            objects.append(obj.object_name)
        return objects

    def bucket_exists(self):
        return self.client.bucket_exists(self.bucket_name)


if __name__ == '__main__':
    config_path = "minio_config.yaml"
    with open(config_path, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    minio_repo = MinioFileRepo(config)

    found = minio_repo.bucket_exists()

    # upload
    is_upload = minio_repo.upload_file(local_path="minio_config.yaml", target_path="aaa/my-test-file.yaml")
    print(is_upload)

    # listdir_object_name
    objects = minio_repo.listdir_object_name(prefix="")
    print(objects)

    # download
    is_download = minio_repo.download_file(local_path="tmp_abc.txt", target_path="abc.txt")
    print(is_download)

    # delete
    is_delete = minio_repo.delete_file(target_path="aaa/my-test-file.yaml")
    print(is_delete)