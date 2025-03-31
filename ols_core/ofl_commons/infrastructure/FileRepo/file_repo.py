from abc import ABC, abstractmethod

class FileRepo(ABC):
    @abstractmethod
    def upload_file(self, local_path: str, target_path: str) -> None:
        pass

    @abstractmethod
    def download_file(self, local_path: str, target_path: str) -> None:
        pass

    @abstractmethod
    def delete_file(self, target_path: str) -> None:
        pass