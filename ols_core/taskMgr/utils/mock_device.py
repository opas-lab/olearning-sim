from typing import Optional, Any
from collections import deque

import json
import uuid
import logging
import hashlib

from ols.ofl_commons.infrastructure.FragmentRepo.fragment_domain import Fragment
from ols.ofl_commons.infrastructure.FileRepo.file_repo import FileRepo
from ols.ofl_commons.infrastructure.TaskRepo.task_repo import TaskRepo


class MockDevice:
    def __init__(self, file_repo: Optional[FileRepo] = None,
        task_repo: Optional[TaskRepo] = None, producer: Optional[Any] = None):
        self.file_repo = file_repo
        self.task_repo = task_repo
        self.producer = producer
        self.message_queue = deque()

    def generate_message(self, task_id, current_round, model_cache_folder: str='', fragment_params=dict(), split_index=0):
        # Generate a simulation message from task.
        fragment = {
            "header": {
                "clientId": hashlib.sha256().hexdigest(),
                "messageId": str(uuid.uuid4()),
                "messageType": "BACK_MODEL_MESSAGE",
            },
            "data": {
                "task_id": task_id,
                "simulation": True,
                "training_round": current_round,
                "local_train_sample_size": fragment_params.get("train_sample_size"),
                "local_test_sample_size": fragment_params.get("test_sample_size"),
                "model": {
                    "framework": "MNN",
                    "payload": f"{model_cache_folder}{uuid.uuid4()}_{split_index}_{task_id}_{current_round}_result_model.mnn"
                  },
                "metrics": {
                    "train_acc_fragment": fragment_params.get("train_acc"),
                    "train_loss_fragment": fragment_params.get("train_loss"),
                    # "train_tp_fragment": [13, 12, 11, 10, 9, 8, 7, 6, 5, 4],
                    # "train_tn_fragment": [17, 16, 15, 10, 9, 8, 7, 6, 5, 4],
                    # "train_fp_fragment": [1, 2, 3, 8, 9, 10, 11, 12, 13, 14],
                    # "train_fn_fragment": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
                    "test_acc_fragment": fragment_params.get("test_acc"),
                    "test_loss_fragment": fragment_params.get("test_loss"),
                    # "test_tp_fragment": [13, 12, 11, 10, 9, 8, 7, 6, 5, 4],
                    # "test_tn_fragment": [17, 16, 15, 10, 9, 8, 7, 6, 5, 4],
                    # "test_fp_fragment": [1, 2, 3, 8, 9, 10, 11, 12, 13, 14],
                    # "test_fn_fragment": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
                    # "thresholds": 10
                }
            }
        }
        fragment = json.dumps(fragment).encode('utf-8')
        self.message_queue.append(fragment)
        return fragment

    def download(self, local_path: str, target_path: str) -> bool:
        return self.file_repo.download_file(local_path=local_path, target_path=target_path)

    def upload(self, local_path: str, fragment: Fragment) -> bool:
        return self.file_repo.upload_file(local_path = local_path, target_path = fragment.data.model.payload)

    def send(self, fragment) -> bool:
        try:
            self.producer.send(self.message_queue.pop())
            return True
        except Exception as e:
            logging.error(f"[Mock Device]: Send model fragment failed! Fragment is {fragment}, because {e}")
            return False
