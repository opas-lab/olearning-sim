from abc import ABC, abstractmethod
from argparse import ArgumentParser, Namespace
from typing import Dict, Any

class OperatorRunScriptABC(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def get_args(self) -> Namespace:
        '''
        对执行脚本进行解析
        '''
        parser = ArgumentParser()
        parser.add_argument("--params", type=str, help="params for training")
        args = parser.parse_args()
        return args

    @abstractmethod
    def run(self):
        import json
        args = self.get_args()
        params_jsonpath = args.params
        with open(params_jsonpath, "r") as f_json:
            run_script_params = json.load(f_json)
        self.execution(run_script_params=run_script_params)
        pass

    def execution(self, run_script_params: Dict[str, Any]):
        pass


