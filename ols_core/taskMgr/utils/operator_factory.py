import sys
from abc import ABC, abstractmethod

# 抽象类
class Operator(ABC):
    def __init__(self, params):
        self.name = params.get("name", "")
        self.operator_code_path = params.get("operator_code_path", "")
        self.operator_entry_file = params.get("operator_entry_file", "")
        self.number_phones = params.get("simulation_params", {}).get("number_phones", 0)
        self.cpu = params.get("simulation_params", {}).get("resource_request", {}).get("cpu", 0)
        self.operator_params = params.get("operator_params", {})
        self.phones_for_actors = []
        self.operator_name_list = []
        self.round_idx = 0
        self.params = None
        self.PYTHONPATH = ""

    @abstractmethod
    def construct_run_params(self):
        raise NotImplementedError("run method must be implemented by the concrete subclass.")

class DefaultOperator(Operator):
    def __init__(self, params):
        super().__init__(params)

    def construct_run_params(self):
        run_params = [{
            "training": False, "task_id": self.params.task_id,
            "operator_name": self.name, "operator_script": self.operator_entry_file,
            "python_path": self.PYTHONPATH,
            "params": {}, "phones_for_actor": phones_for_actor
            } for phones_for_actor in self.phones_for_actors]
        return run_params

class Selection(Operator):
    def __init__(self, params):
        super().__init__(params)

    def construct_run_params(self):
        run_params = []
        num_data_splits = 0
        for phones_for_actor in self.phones_for_actors:
            data_split_index = []
            for i in range(phones_for_actor):
                data_split_index.append(num_data_splits + i)
            num_data_splits = num_data_splits + phones_for_actor
            params = {
                "task_id": self.params.task_id,  "round": self.round_idx,
                "result_folder": self.params.OPERATOR_RESULTS_DICT.get(self.name, ""),
                "manager_config": self.params.MANAGER_CONFIG, "selection_config": self.params.SELECTION_CONFIG,
                "gradient_house_config": self.params.GRADIENT_HOUSE_CONFIG,
                "gradient_house": {
                    "use_gradient_house": self.params.use_gradient_house,
                    "strategy_gradient_house": self.params.gradient_house_strategy
                }
            }
            run_params_dict = {
                "training": False, "task_id": self.params.task_id,
                "operator_name": self.name, "operator_script": self.operator_entry_file,
                "python_path": self.PYTHONPATH,
                "phones_for_actor": phones_for_actor, "data_split_index": data_split_index,
                "params": params
            }
            run_params.append(run_params_dict)
        return run_params

class Training(Operator):
    def __init__(self, params):
        super().__init__(params)

    def get_run_params_from_json(self, params):
        # 从数据库读入训练相关参数
        batch_size = int(self.operator_params.get("batchSize", 32))
        learning_rate = float(self.operator_params.get("learningRate", 0.01))
        model_cache_path = self.operator_params.get("modelCachePath", "")
        current_round = int(self.operator_params.get("currentRound", 0)) + self.round_idx
        params.update({
            "pre_results_required": False,
            "batch_size": batch_size, "learning_rate": learning_rate,
            "model_cache_path": model_cache_path,
            "current_round": current_round
        })
        # 构造用于训练的values列表，其中包含部分训练参数
        run_params = []
        num_data_splits = 0
        for phones_for_actor in self.phones_for_actors:
            data_split_index = []
            for i in range(phones_for_actor):
                data_split_index.append(num_data_splits + i)
            num_data_splits = num_data_splits + phones_for_actor
            run_params_dict = {
                "training": True, "task_id": self.params.task_id,
                "operator_name": self.name, "operator_script": self.operator_entry_file,
                "python_path": self.PYTHONPATH,
                "phones_for_actor": phones_for_actor,
                "data_split_index": data_split_index,
                "params": params
            }
            run_params.append(run_params_dict)
        return run_params

    def get_run_params_from_pre_results(self, params):
        # 获取前一个算子遗留的结果
        training_index = self.operator_name_list.index(self.name)
        pre_operation_name = self.operator_name_list[training_index-1]
        pre_results_path = self.params.OPERATOR_RESULTS_DICT.get(pre_operation_name, "")
        params.update({
            "pre_results_required": True,
            "pre_results_path": pre_results_path
        })
        if pre_operation_name == "selection":
            # 获得selection配置文件的个数，即通过selection服务的手机个数
            sys.path.append(self.PYTHONPATH)
            from ols.taskMgr.utils.utils_oflserver import getS3SelectionFiles
            selection_jsonfiles = getS3SelectionFiles(
                manager_config = self.params.MANAGER_CONFIG,
                jsonfiles_path = pre_results_path
            )
            phones_num_after_selection = len(selection_jsonfiles)
            if phones_num_after_selection <= 0:
                # logger.error(task_id=self.params.task_id, system_name="Ray Header", module_name="run_task",
                #              message=f"[run]: phones_num_after_selection should larger than 0")
                raise ValueError("phones_num_after_selection should larger than 0")
            if phones_num_after_selection > self.number_phones:
                # logger.error(task_id=self.params.task_id, system_name="Ray Header", module_name="run_task",
                #              message=f"[run]: phones_num_after_selection({phones_num_after_selection}) should not larger than phones_num({self.number_phones}) in training procedure")
                raise ValueError(
                    f"phones_num_after_selection({phones_num_after_selection}) should not larger than phones_num({self.number_phones}) in training procedure")

            params.update({"phones_num_after_selection": phones_num_after_selection})
            phones_for_actors = self.phones_for_actors
            run_params = []
            num_jsonfiles = 0
            for phones_for_actor in phones_for_actors:
                jsonfile_names = []
                for i in range(phones_for_actor):
                    jsonfile_names.append(selection_jsonfiles[num_jsonfiles + i])
                num_jsonfiles = num_jsonfiles + phones_for_actor
                run_params_dict = {
                    "training": True, "task_id": self.params.task_id,
                    "operator_name": self.name, "operator_script": self.operator_entry_file,
                    "python_path": self.PYTHONPATH,
                    "phones_for_actor": phones_for_actor, "jsonfile_names": jsonfile_names,
                    "params": params
                }
                run_params.append(run_params_dict)
            return run_params
        else:
            raise NotImplementedError

    def construct_run_params(self):
        params = dict()
        params.update({
            "task_id": self.params.task_id, "operator_name": self.name,
            "manager_config": self.params.MANAGER_CONFIG, "repo_config": self.params.REPO_CONFIG,
            "ray_local_data_path": self.params.RAY_LOCAL_DATA_PATH,
            "file_transfer_type": self.params.file_transfer_type, "dataset_split_type": self.params.dataset_split_type,
            "training_files": self.params.TRAINING_FILES,
            "phones_num": self.number_phones, "round": self.round_idx,
            "simu_data_zip_path": self.params.simu_data_zip_path,
            "simu_data_path": self.params.simu_data_path,
            "gradient_house_config": self.params.GRADIENT_HOUSE_CONFIG,
            "gradient_house": {
                "use_gradient_house": self.params.use_gradient_house,
                "strategy_gradient_house": self.params.gradient_house_strategy
            }
        })
        # 判断是否需要考虑之前算子的结果
        training_index = self.operator_name_list.index(self.name)
        if training_index == 0: #不需要考虑之前算子的结果
            run_params = self.get_run_params_from_json(params)
        else: #需要考虑之前算子的结果
            run_params = self.get_run_params_from_pre_results(params)
        return run_params

class OperatorFactory:
    @staticmethod
    def create_operator(operator_name=None, params={}):
        if operator_name == "selection":
            return Selection(params)
        elif operator_name == "training":
            return Training(params)
        else:
            return DefaultOperator(params)
