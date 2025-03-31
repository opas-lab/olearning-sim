from abc import ABC, abstractmethod
from typing import Dict, Any
import json
import argparse
import os

class OperatorABC(ABC):
    def __init__(self):
        self.params = {}

    def get_params(self):
        ''' 获取operator执行时从仿真平台分配获取的参数
        example:

        self.params = {
            "task_id": "ols_test", # str, 任务ID
            "config": {
                "python_path": "/home/code/olearning-simulator/python", # str, 仿真代码库路径
                "ray_local_data_path": "/home/ray/tempdata",            # str, ray节点中临时文件存放地址
                "manager_config": "/home/code/olearning-simulator/python/ols/config/manager_config.yaml",       # str, S3, MINIO的服务配置
                "deviceflow_config": "/home/code/olearning-simulator/python/ols/config/deviceflow_config.yaml", # str, Deviceflow的服务配置
                "task_type_config": "/home/code/olearning-simulator/python/ols/config/task_type_config.yaml"    # str, 任务类型的相关配置
            },
            "current_round": 0, # int, 当前算子流轮次
            "data": {
                "name": "data_0",              # str, 数据名称
                "data_path": "/home/ray/tempdata/ols_test_game/actor_4246a3d6-c0ab-4c2b-9373-69c6c4ec3e08/data/data/868607044936436.gz", # str, 分配到的数据路径
                "data_split_type": true,       # bool, 数据已被用户进行划分为true, 数据未被用户进行划分为false
                "task_type": "classification", # str, 任务类型
                "dataconfig": {}               # dict, 数据的相关配置，无特殊配置一般为空字典
            },
            "operator": {
                "name": "training",                  # str, 算子名
                "operation_behavior_controller": {
                    "use_gradient_house": bool,      # str, 是否使用梯度中间站Deviceflow
                    "strategy_gradient_house": "",   # str, Deviceflow策略，一般为json型字符串，不使用Deviceflow时为空
                    "outbound_service": ""           # str, 云服务连接配置，一般为json型字符串，算子不连接云服务时为空
                },
                "input": [],              # list, 此算子执行依赖的之前的算子
                "use_data": false,        # bool, 是否使用数据，准确来说是否需要在单个actor循环执行前下载数据。
                                          # 如果设备多但数据总量小，建议在创建任务时选true；如果单个数据容量大，那么建议选false, 并在执行脚本内部下载数据。
                "model": {
                    "use_model": false,   # bool, 是否使用模型，准确来说是否需要在单个actor循环执行前下载模型。
                                          # 如果使用模型时需要多个文件，那么建议创建任务时选false, 然后配置算子时增加下载文件的算子
                    "model_path": ""      # str, 模型路径，如果模型提前下载后此参数即为模型本地路径
                },
                "operator_params": ""     # str, 算子参数，一般为json型字符串，否则为空
            },
            "actor_save_dir": "/home/ray/tempdata/ols_test_game/actor_4246a3d6-c0ab-4c2b-9373-69c6c4ec3e08" # str, actor的保存路径
            "actor_simulation_num": 1,    # int, 每个actor需要模拟的设备数量
            "params": {}                  # dict, 算子参数的json解析，否则为空字典,
        }
        '''
        try:
            parser = argparse.ArgumentParser()
            parser.add_argument("--params", type=str, default="", help="params for operator")
            args = parser.parse_args()
            params_jsonpath = args.params
            with open(params_jsonpath, "r") as f_json:
                params = json.load(f_json)
            self.params = params
        except Exception as e:
            raise Exception(f"[OperatorABC] get_params failed, because of {e}")


    def clean_temp_files(self) -> None:
        '''
        清除和operator_run_script脚本执行相关的临时文件
        '''
        operator_name = self.params.get("operator", {}).get("name", "")
        data_name = self.params.get("data", {}).get("name", "")
        split_index = self.params.get("split_index", 0)
        run_params_json = f"{operator_name}_{data_name}_{split_index}.json"
        try:
            os.remove(run_params_json)
        except:
            pass


    def raise_expection(self, res_status: int) -> None:
        operator = self.params.get("operator", {})
        operator_name = operator.get("name", "")
        raise Exception(f"operator = {operator_name} execute failed, res_status = {res_status}")


    def execute_run_script(self, run_script: str) -> int:
        '''
        执行operator_run_script脚本，返回执行状态res_status
        '''
        res_status = os.system(run_script)
        return res_status


    @abstractmethod
    def construct_run_params(self) -> Dict[str, Any]:
        '''
        根据依赖的算子输入input，以及self.params，自行组装actor执行的operator_run_script的依赖参数，返回Dict
        '''
        run_params = {}
        return run_params


    @abstractmethod
    def construct_run_script(self, run_params: Dict[str, Any], run_script_file: str) -> str:
        '''
        根据run_params, 构造用于系统执行的run_script字符串

        example:

        run_script = f"python3 {operator_name}/game_single_train.py --split_index {split_index} --params {run_params_file} --model mlp --task_type only_device"
        '''
        operator = self.params.get("operator", {})
        operator_name = operator.get("name", "")
        data = self.params.get("data", {})
        split_index = self.params.get("split_index", 0)
        data_name = data.get("name", "")

        run_params_json = f"{operator_name}_{data_name}_{split_index}.json"
        with open(run_params_json, "w") as json_file:
            json.dump(run_params, json_file)

        run_script = f"python3 {operator_name}/{run_script_file} --params {run_params_json}"
        return run_script


    @abstractmethod
    def run(self):
        self.get_params()
        run_params = self.construct_run_params()
        # TODO
        run_script_file = "xxx.py"
        run_script = self.construct_run_script(run_params=run_params, run_script_file=run_script_file)
        res_status = self.execute_run_script(run_script=run_script)
        self.clean_temp_files()
        if res_status != 0: #如果出现的执行异常
            self.raise_expection(res_status=res_status)