import os
import json
import yaml
import pandas as pd
from ols.simu_log import Logger

import redis

# filepath
FILEPATH = os.path.abspath(os.path.dirname(__file__))   # utils.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '../..')) # ols所在文件夹的绝对路径
# log
logger = Logger()
logger.set_logger(log_yaml = OLSPATH + "/config/repo_log.yaml")

class RedisRepo:
    def __init__(self, yaml_path:str):
        with open(yaml_path, 'r') as file:
            yaml_config = yaml.load(file, Loader=yaml.FullLoader)
        #self._password = task_table_config["password"]
        self._host = str(yaml_config["host"])
        self._port = int(yaml_config["port"])
        self._list_key = str(yaml_config["list_key"])
        self._redis = redis.Redis(host=self._host, port=self._port, decode_responses=True)
        print("read yaml info:")
        print(self._host, self._port, self._list_key)

    # 新增数据
    def insert_data(self, value):
        try:
            self._redis.rpush(self._list_key, value)
            return True
        except Exception as e:
            print(f"insert err, key: {self._list_key}, value: {value}")
            print(e)
            logger.error(task_id="", system_name="TaskMgr", module_name="Repo",
                    message=f"[insert_data]:   value={value} failed, because of {e}")
            return False

    # 从队列最左边读取并删除一个元素
    def pop_data(self):
        try:
            # TODO 从左边读取并删除元素
            element = self._redis.lpop(self._list_key)
            return element
        except Exception as e:
            return None

