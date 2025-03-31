from typing import Optional, Dict, Any
import logging
import os
import requests
import base64

from ols.ofl_commons.infrastructure.FileRepo.file_repo import FileRepo


class HttpFileRepo(FileRepo):
    def __init__(self, config: Dict[str, Any]):
        """
        config: Dict[str, Any], e.g.
        {
            'root_url': 'http://127.0.0.1
        """
        self._root_url = config.get('root_url', 'http://127.0.0.1')
        self._upload_url = os.path.join(self._root_url, 'uploadFile')
        self._download_url = os.path.join(self._root_url, 'downloadFile')
        self._delete_url = os.path.join(self._root_url, 'deleteFile')
        self._check_url = os.path.join(self._root_url, 'existFile')

        self._file_type = config.get('file_type', 'TRAIN')

        logging.debug(
            f'''HttpFileRepo init with: 
            upload_url {self._upload_url},\n
            download_url {self._download_url},\n
            delete_url {self._delete_url},\n
            file_type {self._file_type},\n
            ''')


    def upload_file(self, local_path: str, target_path: str) -> bool:
        try:
            with open(local_path, 'rb') as f:
                r = requests.post(
                    url = self._upload_url,
                    files = {
                        'files': (target_path, f),
                        'path': (None, ''),
                        'file_type': (None, self._file_type)
                    }
                )
            logging.debug(f'upload file {local_path} to {target_path} with response {r.json()}')
            # parse response
            response_code = r.json().get('code', -1)
            if response_code != 0:
                logging.error(f'upload to {target_path} failed with response {r.json()}')
                return False
        except Exception as e:
            logging.exception(e)
            return False
    
        return True


    def download_file(self, local_path: str, target_path: str) -> bool:
        # make request multipart/form-data
        try:
            r = requests.post(
                url = self._download_url,
                data = {
                    'file_key': target_path,
                    'file_type': self._file_type
                }
            )
            logging.debug(f'download file to {local_path} from {target_path}')
            # check success
            response_code = r.json().get('code', -1)
            if response_code != 0:
                logging.error(f'download failed with response {r.json()}')
                return False

            # get data and write to local_path
            b64_data = r.json()['data']
            decoded_data = base64.b64decode(b64_data)
            with open(local_path, 'wb') as f:
                f.write(decoded_data)
        except Exception as e:
            logging.exception(e)
            return False

        return True


    def delete_file(self, target_path: str) -> bool:
        try:
            r = requests.post(
                url = self._delete_url,
                data = {
                    'file_key': target_path,
                    'file_type': self._file_type
                }
            )
            logging.debug(f'delete file {target_path}')
            # check success
            response_code = r.json().get('code', -1)
            if response_code != 0:
                logging.error(f'delete failed with response {r.json()}')
                return False
        except Exception as e:
            logging.exception(e)
            return False

        return True

    def check_file(self, target_path: str) -> bool:
        try:
            r = requests.post(
                url = self._check_url,
                data = {
                    'file_key': target_path,
                    'file_type': self._file_type
                }
            )
            logging.debug(f'check file {target_path}')
            logging.info(f"check file response is: {r.json()}")
            # check success
            resource_code = r.json().get('code', -1)
            if resource_code != 0:
                logging.error(f'check failed with response {r.json()}')
                return False
        except Exception as e:
            logging.exception(e)
            return False
        return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    root_url = "http://dcc-tunnel-cn-test.wanyol.com/server/api/v1/"
    repo = HttpFileRepo(config = {
        'root_url': root_url
    })

    with open('1.txt', 'w') as f:
        f.write('hello http storage!^_^\n')

    repo.upload_file('1.txt', '20230515/testtest/hello_storage.txt')
    repo.download_file('2.txt', '20230515/testtest/hello_storage.txt')
    repo.check_file('20230515/testtest/hello_storage.txt')
    repo.delete_file('20230515/testtest/hello_storage.txt')
    repo.download_file('2.txt', '20230515/testtest/hello_storage.txt')
    repo.check_file('20230515/testtest/hello_storage.txt')