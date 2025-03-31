import os
import ray
import sys
import grpc
import yaml
import json
import uuid
import time
import shutil
import zipfile
import logging

# extra import
from ols.proto import gradient_house_pb2 as gradient_house__pb2
from ols.proto import gradient_house_pb2_grpc
from ols.proto import taskService_pb2 as taskService__pb2
from ols.taskMgr.utils.utils import TaskTableRepo
from ols.simu_log import Logger

class Params:
    def __init__(self, params=dict()):
        self.PYTHONPATH = params.get("python_path", "")
        # log
        self.logger = Logger()
        self.logger.set_logger(log_yaml=self.PYTHONPATH + "/ols/config/repo_log.yaml")
        # 一些固定参数
        self.RAY_LOCAL_DATA_PATH = params.get("ray_local_data_path", "")
        self.MANAGER_CONFIG = f"{self.PYTHONPATH}/ols/config/manager_config.yaml"
        self.REPO_CONFIG = f"{self.PYTHONPATH}/ols/config/repo_config.yaml"
        self.SELECTION_CONFIG = f"{self.PYTHONPATH}/ols/config/selection_config.yaml"
        self.REPO_LOG = f"{self.PYTHONPATH}/ols/config/repo_log.yaml"
        self.TASK_TABLE_PATH = f"{self.PYTHONPATH}/ols/config/taskmgr_table.yaml"
        self.GRADIENT_HOUSE_CONFIG = f"{self.PYTHONPATH}/ols/config/gradient_house_config.yaml"
        self.TRAINING_FILES = {
            "train_features": "train_features.csv",
            "train_labels": "train_labels.csv",
            "test_features": "test_features.csv",
            "test_labels": "test_labels.csv"
        }
        self.OPERATOR_RESULTS_DICT = dict()
        self.extract_params(params=params)

    def extract_params(self, params=dict()):
        self.task_id = params.get("task_id", "")
        self.simu_data_zip_path = params.get("simu_data_path", "")
        self.simu_data_path = ""
        file_transfer_type_str = params.get("file_transfer_type", "FILE")  # 文件传输方式
        self.file_transfer_type = eval(f"taskService__pb2.FileTransferType.{file_transfer_type_str}")
        self.dataset_split_type = params.get("dataset_split_type", False)  # 数据集划分方式
        self.round = params.get("round", 0)
        self.use_gradient_house = params.get("gradient_house", {}).get("use_gradient_house", False)
        self.gradient_house_strategy = params.get("gradient_house", {}).get("strategy_gradient_house", "")
        self.operators = params.get("operators", [])
        self.operator_name_list = []
        self.task_repo = TaskTableRepo(yaml_path=self.TASK_TABLE_PATH)
        self.results_upload_path = f"{self.task_id}/tmp_results" # S3

class GradientHouse:
    def __init__(self, use=False, strategy="", task_id = "", config_path="", logger=None):
        self._use = use
        self._strategy = strategy
        self._task_id = task_id
        with open(config_path, 'r') as file:
            config = yaml.load(file, Loader=yaml.FullLoader)
        self._grpc_path = config["grpc_service_address"]
        self._logger = logger

    def start(self):
        if self._use:
            try:
                with grpc.insecure_channel(self._grpc_path) as channel:
                    stub = gradient_house_pb2_grpc.GradientHouseStub(channel)
                    response = stub.StartTask(gradient_house__pb2.StartTaskRequest(
                        task_id = self._task_id,
                        strategy_name = self._strategy
                    ))
                return True
            except Exception as e:
                self._logger.error(task_id=self._task_id, system_name="Ray Runner", module_name="GradientHouse",
                             message=f"[start]: use StartTask of Gradient House failed, because of {e}")
                return False
        else:
            return True

    def stop(self):
        if self._use:
            try:
                with grpc.insecure_channel(self._grpc_path) as channel:
                    stub = gradient_house_pb2_grpc.GradientHouseStub(channel)
                    response = stub.StopTask(gradient_house__pb2.StopTaskRequest(task_id = self._task_id))
                return True
            except Exception as e:
                self._logger.error(task_id=self._task_id, system_name="Ray Runner", module_name="GradientHouse",
                             message=f"[stop]: use StopTask of Gradient House failed, because of {e}")
                return False
        else:
            return True

@ray.remote(num_cpus=1)
class Actor:
    def __init__(self):
        pass

    def get_params(self, value):
        self.PYTHONPATH = value.get("python_path", "")
        sys.path.append(self.PYTHONPATH)
        self.task_id = value.get("task_id", "")
        self.success_num = 0
        self.failed_num = 0
        self.operator_name = value.get("operator_name", "")
        self.operator_script = value.get("operator_script", "")
        self.phones_for_actor = value.get("phones_for_actor", 0)
        self.params = value.get("params", {})
        self.data_split_index = value.get("data_split_index", [])
        self.training_mode = value.get("training", False)
        self.round_idx = self.params.get("round", 0)

    def _single_compute_step(self, task_id, operator_name, operator_script,
                             python_path="", split_index=None, params=dict()):
        params_path = ""
        if split_index is None:
            if params:
                params_path = f"{uuid.uuid4()}.json"
                with open(params_path, "w") as json_file:
                    json.dump(params, json_file)
                run_script = f"python3 {operator_name}/{operator_script} --params " + params_path
            else:
                run_script = f"python3 {operator_name}/{operator_script}"
        else:
            if params:
                params_path = f"{uuid.uuid4()}.json"
                with open(params_path, "w") as json_file:
                    json.dump(params, json_file)
                run_script = f"python3 {operator_name}/{operator_script} --split_index {split_index} --params " + params_path
            else:
                run_script = f"python3 {operator_name}/{operator_script} --split_index {split_index}"
        print(f"run_script = {run_script}")
        os.putenv("PYTHONPATH", python_path)
        res_status = os.system(run_script)
        if params:
            try:
                os.remove(params_path)
            except Exception as e:
                print(f"[_single_compute_step]: Delete params_path={params_path} failed!")
        return res_status

    def download_data_files(self):
        file_transfer_type = self.params.get("file_transfer_type", 0)
        dataset_split_type = self.params.get("dataset_split_type", False)
        phones_num = self.params.get("phones_num", 0)
        simu_data_zip_path = self.params.get("simu_data_zip_path", "")
        ray_local_data_path = self.params.get("ray_local_data_path", "")
        if file_transfer_type == 0: #本地（共享文件服务器）存在数据zip包
            if os.path.exists(simu_data_zip_path):
                local_zipfile_path = simu_data_zip_path
            else:
                raise Exception(f"[Actor, download_data_files]: local simu_data_zip_path={simu_data_zip_path} is not exist.")
        else: #不在本地
            local_zipfile_dir = f"{ray_local_data_path}/{self.task_id}"
            os.makedirs(local_zipfile_dir, exist_ok=True, mode=0o777)
            zipfilename = os.path.basename(simu_data_zip_path)
            local_zipfile_path = f"{local_zipfile_dir}/{zipfilename}"

            if file_transfer_type == 1: #需要http请求下载
                import wget
                import urllib.request
                try:
                    with urllib.request.urlopen(simu_data_zip_path, timeout=10) as response:
                        headers = response.info()
                        content_length = int(response.headers['Content-Length'])
                    if content_length > 0:
                        wget.download(simu_data_zip_path, out=local_zipfile_path)
                    else:
                        raise Exception(f"[Actor, download_data_files]: zipfile={simu_data_zip_path} is empty!")
                except Exception as e:
                    raise Exception(f"[Actor, download_data_files]: Download zipfile={simu_data_zip_path} from http link failed, because of {e}")

            elif file_transfer_type == 2: #需要从S3下载
                from ols.taskMgr.utils.utils_oflserver import downloadS3File
                isDownloadZipFile = downloadS3File(
                    manager_config=self.params.get("manager_config", ""),
                    local_file=local_zipfile_path,
                    taget_file=simu_data_zip_path
                )
                if isDownloadZipFile == False:
                   raise Exception(f"[Actor, download_data_files]: Download zipfile={simu_data_zip_path} from S3 failed")

        # 解压下载的zip包
        tmp_taskid_data_path = f"{ray_local_data_path}/{self.task_id}/data"
        os.makedirs(tmp_taskid_data_path, exist_ok=True, mode=0o777)
        zip_file = zipfile.ZipFile(local_zipfile_path, "r")
        zip_file.extractall(path=tmp_taskid_data_path)
        zip_file.close()
        extract_path_final = tmp_taskid_data_path
        data_subfolder_name = os.listdir(extract_path_final)
        if dataset_split_type == False:
            if len(data_subfolder_name) != 1:
                raise AssertionError(f"[Actor, download_data_files]: length of data_subfolder_name({len(data_subfolder_name)}) is not equal to 1, dataset_split_type={dataset_split_type}")
            simu_data_path = f"{extract_path_final}/{data_subfolder_name[0]}"
        else:
            simu_data_path = extract_path_final

        self.params.update({"simu_data_path": simu_data_path})

        # 获得子文件夹名
        if dataset_split_type == True:
            subfolder_names = os.listdir(simu_data_path)
            if len(subfolder_names) != phones_num:
                raise AssertionError(f"[Actor, download_data_files]: length of subfolder_names({len(subfolder_names)}) is not equal to phones_num({phones_num}), dataset_split_type={dataset_split_type}")
            simu_data_subfolders = []
            for subfolder_name in sorted(subfolder_names):
                subfolder_path = f"{simu_data_path}/{subfolder_name}"
                simu_data_subfolders.append(subfolder_path)
            self.params.update({"simu_data_subfolders": simu_data_subfolders})

    def download_pre_results(self, value):
        jsonfile_names = value.get("jsonfile_names", [])
        local_folder = f"./{self.task_id}/round{self.round_idx}/{self.operator_name}" #前一个算子结果的下载路径
        os.makedirs(local_folder, exist_ok=True, mode=0o777)
        data_split_index, params_new_list = [], []
        for ind in range(self.phones_for_actor):
            jsonfile_name = jsonfile_names[ind]
            from ols.taskMgr.utils.utils_oflserver import downloadS3File, deleteS3File
            local_file = f"{local_folder}/{os.path.basename(jsonfile_name)}"
            isDownload = downloadS3File(
                manager_config=self.params.get("manager_config", ""),
                local_file=local_file,
                taget_file=jsonfile_name
            )
            if isDownload == False:
                raise Exception(f"[run_task]: Download {jsonfile_name} failed")

            with open(local_file, 'r') as fp:
                jsondata = json.load(fp)

            train_parameters_data = jsondata.get("data", {}).get("train_parameters_data", {})
            split_index = jsondata.get("data", {}).get("split_index", -1)
            params_new = self.params
            params_new.update({
                "batch_size": train_parameters_data.get("batch_size", 64),
                "learning_rate": train_parameters_data.get("learning_rate", 0.01),
                "model_cache_path": train_parameters_data.get("model_cache_path", ""),
                "current_round": jsondata.get("data").get("round_idx", -1)
            })
            data_split_index.append(split_index)
            params_new_list.append(params_new)
            # 删除临时文件
            isDeleted = deleteS3File(manager_config=self.params.get("manager_config", ""), taget_file=jsonfile_name)
            if isDeleted == False:
                raise Exception(f"[run_task]: Delete {jsonfile_name} in S3 failed")
        self.data_split_index = data_split_index
        return params_new_list

    def delete_tmp_results(self):
        try:
            time.sleep(1)
            local_data_dir = f"{self.params.ray_local_data_path}/{self.task_id}"
            shutil.rmtree(local_data_dir)
            return True
        except Exception as e:
            return False

    def loop_run(self, params_list=[]):
        for ind in range(self.phones_for_actor):
            if self.data_split_index:
                split_index = self.data_split_index[ind]
            else:
                split_index = None
            if params_list:
                params = params_list[ind]
            else:
                params = self.params
            print(f"self.PYTHONPATH = {self.PYTHONPATH}")
            res_status = self._single_compute_step(
                task_id = self.task_id,
                operator_name = self.operator_name,
                operator_script = self.operator_script,
                python_path = self.PYTHONPATH,
                split_index = split_index,
                params = params
            )
            if res_status == 0:
                self.success_num += 1
            else:
                self.failed_num += 1

    def run(self, value):
        self.get_params(value)
        try:
            if self.training_mode: #训练
                pre_results_required = self.params.get("pre_results_required", False)
                self.download_data_files() #下载数据文件并解压
                if pre_results_required: #如果需要依赖之前的结果
                    params_list = self.download_pre_results(value)
                    self.loop_run(params_list=params_list)
                else:
                    self.loop_run()
                self.delete_tmp_results() # 删除本地下载和解压的文件夹
            else:
                if len(self.data_split_index) != 0:
                    if len(self.data_split_index) != self.phones_for_actor:
                        raise ValueError(
                            f"length of data_split_index ({len(self.data_split_index)}) is not equal to phones_for_actor({self.phones_for_actor})")
                    self.loop_run()
                else:
                    self.loop_run()
            res_num = {"success": self.success_num, "failed": self.failed_num}
            return res_num
        except Exception as e:
            res_num = {"success": 0, "failed": self.phones_for_actor}
            logging.error(f"[run]: Actor run failed, because of {e}")
            return res_num



