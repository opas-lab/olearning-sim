import json
import logging
from typing import List, Dict, Any

# 数据库及日志
import os
from ols.simu_log import Logger
from ols.utils.repo_utils import SqlDataBase
FILEPATH = os.path.abspath(os.path.dirname(__file__))             # registry.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '../..'))        # ols所在文件夹的绝对路径
DEVICEFLOW_TABLE_PATH = OLSPATH + "/config/deviceflow_table.yaml" # device_table_path
LOG_PATH = OLSPATH + "/config/repo_log.yaml"                      # log日志配置的文件路径

class TaskOrientedDeviceFlowRegistry:
    _instance = None
    _task_registry: Dict[str, Dict[str, Any]] = {}
    def __new__(cls, *args, **kw):
        if cls._instance is None:
            cls._instance = object.__new__(cls, *args, **kw)
        return cls._instance

    def __init__(self):
        self.sql_table = SqlDataBase(yaml_path=DEVICEFLOW_TABLE_PATH)
        self.logger = Logger()
        self.logger.set_logger(log_yaml=LOG_PATH)

    def register_task(self, task_id: str, total_compute_resources: List[str]) -> bool:
        """
        Register a task with its compute resources.

        Args:
            task_id (str): The task identifier.
            total_compute_resources (List[str]): The list of compute resources for the task.

        Returns:
            bool: True if the task was registered successfully, False if the task was already registered.
        """
        if task_id in self._task_registry:
            self.logger.info(task_id=task_id, system_name="Deviceflow", module_name="register_task",
                              message=f"[add_item]: Task ={task_id} already registered, thus here do nothing")
            return False
        try:
            total_compute_resources = {
                "total_compute_resources": list(total_compute_resources)
            }
            # 数据库持久化
            if self.sql_table.check_task_in_database(identify_name="task_id", identify_value = task_id):
                # TODO: 得做专门调整，即需要满足task_id=task_id但flow_id为None的条目，再进行修改
                # 如果task_id在数据库里, 则需要改值
                if not self.sql_table.set_item_value(
                    identify_name = "task_id", identify_value = task_id,
                    item = "task_registry", value = json.dumps(total_compute_resources),
                    log_params={"task_id": task_id, "system_name": "Deviceflow", "module_name": "register_task"}
                ):
                    self.logger.error(task_id=task_id, system_name="Deviceflow", module_name="register_task",
                        message=f"[set_item_value]: Failed to set task_registry={json.dumps(total_compute_resources)} "
                                f"of task_id={task_id} in repo.")
                    return False
                else:
                    self._task_registry[task_id] = total_compute_resources
            else:
                # 如果task_id在不在数据库里, 则需要加一条数据目录
                if not self.sql_table.add_item(
                    item={"task_id": [task_id], "task_registry": [json.dumps(total_compute_resources)]},
                    log_params={"task_id": task_id, "system_name": "Deviceflow", "module_name": "register_task"}
                ):
                    self.logger.error(task_id=task_id, system_name="Deviceflow", module_name="register_task",
                          message=f"[add_item]: Failed to add task_registry={json.dumps(total_compute_resources)} "
                                  f"of task_id={task_id} in repo.")
                    return False
                else:
                    self._task_registry[task_id] = total_compute_resources

        except Exception as e:
            self.logger.error(task_id=task_id, system_name="Deviceflow", module_name="register_task",
                              message=f"[add_item]: Failed to register task of task_id={task_id} because of {e}")
            return False
        else:
            return True


    def unregister_task(self, task_id: str) -> bool:
        if task_id not in self._task_registry:
            self.logger.info(task_id=task_id, system_name="Deviceflow", module_name="unregister_task",
                              message=f"[delete_item]: Task ={task_id} not registered, thus here do nothing")
            return False
        else:
            self._task_registry.pop(task_id, None)
            return True

    def get_total_compute_resources(self, task_id: str) -> List[str]:
        """
        Get the total compute resources for a given task_id.

        Args:
            task_id (str): The task identifier.

        Returns:
            List[str]: The list of compute resources for the task, or an empty list if not found.
        """
        return self._task_registry.get(task_id, {}).get("total_compute_resources", [])

    def save_state(self, filepath: str):
        with open(filepath, 'w') as f:
            json.dump(self._task_registry, f)

    def load_state(self, filepath: str):
        try:
            with open(filepath, 'r') as f:
                self._task_registry = json.load(f)
        except FileNotFoundError:
            self._task_registry = {}
