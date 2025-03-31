import copy
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

import grpc
from ols.proto import deviceflow_pb2, deviceflow_pb2_grpc

from ols.proto import taskService_pb2 as taskService__pb2
from ols.taskMgr.utils.utils import TaskTableRepo

class Params:
    def __init__(self, params=dict(), set_logger=True, extract_params=True):
        self.PYTHONPATH = params.get("python_path", "")
        # 一些固定参数
        self.RAY_LOCAL_DATA_PATH = params.get("ray_local_data_path", "")
        self.MANAGER_CONFIG = f"{self.PYTHONPATH}/ols/config/manager_config.yaml"
        self.REPO_CONFIG = f"{self.PYTHONPATH}/ols/config/repo_config.yaml"
        self.REPO_LOG = f"{self.PYTHONPATH}/ols/config/repo_log.yaml"
        self.TASK_TABLE_PATH = f"{self.PYTHONPATH}/ols/config/taskmgr_table.yaml"
        self.DEVICEFLOW_CONFIG = f"{self.PYTHONPATH}/ols/config/deviceflow_config.yaml"
        self.TASK_TYPE_CONFIG = f"{self.PYTHONPATH}/ols/config/task_type_config.yaml"
        if set_logger:
            # log
            from ols.simu_log import Logger
            self.logger = Logger()
            self.logger.set_logger(log_yaml=self.PYTHONPATH + "/ols/config/repo_log.yaml")
        if extract_params:
            self.extract_params(params=params)

    def extract_params(self, params=dict()):
        # 参数解析
        self.user_id = params.get("user_id", "")
        self.task_id = params.get("task_id", "")
        self.target_data_list = params.get("target", {}).get("data", [])
        self.operatorflow_setting = params.get("operatorflow", {}).get("flow_setting", [])
        self.operator_list = params.get("operatorflow", {}).get("operators", [])
        self.computation_unit = params.get("logical_simulation", {}).get("computation_unit", {})
        self.resource_request_list = params.get("logical_simulation", {}).get("resource_request", [])
        # 其他设置
        self.current_round = 0
        self.task_repo = TaskTableRepo(yaml_path=self.TASK_TABLE_PATH)
        self.results_upload_path = f"{self.task_id}/tmp_results"  # S3

class Deviceflow:
    def __init__(self, use=False, routing_key="",
                 strategy="", compute_resource="logical_simulation",
                 outbound_service = deviceflow_pb2.OutboundService(),
                 task_id="", config_path="", logger=None):
        self._use = use
        self._routing_key = routing_key
        self._strategy = strategy
        self._compute_resource = compute_resource
        self._outbound_service = outbound_service
        self._task_id = task_id
        self._logger = logger
        if use == True:
            with open(config_path, 'r') as file:
                config = yaml.load(file, Loader=yaml.FullLoader)
            self._grpc_path = config["grpc_service_address"]
        else:
            self._grpc_path = ""

    def start(self):
        if self._use:
            try:
                with grpc.insecure_channel(self._grpc_path) as channel:
                    deviceflow_stub = deviceflow_pb2_grpc.TaskOperatorOrientedDeviceFlowStub(channel)
                    response = deviceflow_stub.NotifyStart(
                        deviceflow_pb2.NotifyRequest(
                            task_id = self._task_id,
                            routing_key = self._routing_key,
                            outbound_service = self._outbound_service,
                            strategy = self._strategy,
                            compute_resource = self._compute_resource
                        )
                    )
                    if response.is_success:
                        return True
                    else:
                        return False
            except Exception as e:
                self._logger.error(task_id=self._task_id, system_name="Ray Runner", module_name="Deviceflow",
                             message=f"[start]: use NotifyStart of Deviceflow failed, because of {e}")
                return False
        else:
            return True

    def stop(self):
        if self._use:
            try:
                with grpc.insecure_channel(self._grpc_path) as channel:
                    deviceflow_stub = deviceflow_pb2_grpc.TaskOperatorOrientedDeviceFlowStub(channel)
                    response = deviceflow_stub.NotifyComplete(
                        deviceflow_pb2.NofifyCompleteRequest(
                            task_id = self._task_id,
                            routing_key = self._routing_key,
                            compute_resource = self._compute_resource
                        )
                    )
                    if response.is_success:
                        return True
                    else:
                        return False
            except Exception as e:
                self._logger.error(task_id=self._task_id, system_name="Ray Runner", module_name="Deviceflow",
                             message=f"[stop]: use NotifyComplete of Deviceflow failed, because of {e}")
                return False
        else:
            return True

    def get_deviceflow_pulsar_client(self):
        try:
            with grpc.insecure_channel(self._grpc_path) as channel:
                deviceflow_stub = deviceflow_pb2_grpc.TaskOperatorOrientedDeviceFlowStub(channel)
                deviceflow_pulsar_client = deviceflow_stub.GetDeviceflowPulsarClient(
                    deviceflow_pb2.google_dot_protobuf_dot_empty__pb2.Empty()
                )
            return deviceflow_pulsar_client
        except Exception as e:
            self._logger.error(task_id=self._task_id, system_name="Ray Runner", module_name="Deviceflow",
                               message=f"[get_deviceflow_pulsar_client]: get_deviceflow_pulsar_client failed, because of {e}")
            return None

    def get_deviceflow_websocket(self):
        try:
            with grpc.insecure_channel(self._grpc_path) as channel:
                deviceflow_stub = deviceflow_pb2_grpc.TaskOperatorOrientedDeviceFlowStub(channel)
                deviceflow_websocket = deviceflow_stub.GetDeviceflowWebsocket(
                    deviceflow_pb2.google_dot_protobuf_dot_empty__pb2.Empty()
                )
            return deviceflow_websocket
        except Exception as e:
            self._logger.error(task_id=self._task_id, system_name="Ray Runner", module_name="Deviceflow",
                               message=f"[get_deviceflow_websocket]: get_deviceflow_websocket failed, because of {e}")
            return None

@ray.remote(num_cpus=1)
class Actor:
    def __init__(self):
        self.actor_id = f"actor_{uuid.uuid4()}"
        self.PYTHONPATH = ""
        self.task_id = ""
        self.success_num = 0
        self.failed_num = 0
        self.operator_name = ""
        self.operator_script = ""
        self.simulation_num = 0
        self.split_index_list = []
        self.current_round = 0
        self.run_params = dict()

    def get_params(self, run_params):
        self.PYTHONPATH = run_params.get("config", {}).get("python_path", "")
        sys.path.append(self.PYTHONPATH)
        self.task_id = run_params.get("task_id", "")
        self.success_num = 0
        self.failed_num = 0
        self.operator_name = run_params.get("operator", {}).get("name", "")
        self.operator_script = run_params.get("operator", {}).get("logical_simulation", {}).get("operator_entry_file", "")
        self.simulation_num = run_params.get("simulation_num", 0)
        self.split_index_list = run_params.get("split_index_list", [])
        self.current_round = run_params.get("current_round", 0)
        self.run_params = run_params # 将run_params赋值到Actor类的固有属性，方便后续对其增加一些属性值

    def download_data_files(self, local_dir, config, data):
        data_transfer_type = data.get("data_transfer_type", "S3")
        data_split_type = data.get("data_split_type", False)
        simu_data_zip_path = data.get("data_path", "")
        # 表明无需下载数据文件, 则直接退出下载数据过程
        if simu_data_zip_path == "":
            return True
        try:

            _, file_ext = os.path.splitext(simu_data_zip_path)
            # print(f"simu_data_zip_path = {simu_data_zip_path}")
            # print(f"file_ext = {file_ext}")
            if file_ext == ".zip":
                need_download = True
            else:
                need_download = False

            # print(f"need_download = {need_download}")

            if need_download:
                # 需要下载数据文件
                if data_transfer_type == "FILE": #本地（共享文件服务器）存在数据zip包
                    if os.path.exists(simu_data_zip_path):
                        local_zipfile_path = simu_data_zip_path
                    else:
                        raise Exception(f"[Actor, download_data_files]: local simu_data_zip_path={simu_data_zip_path} is not exist.")
                else: #不在本地
                    os.makedirs(local_dir, exist_ok=True, mode=0o777)
                    zipfilename = os.path.basename(simu_data_zip_path)
                    local_zipfile_path = f"{local_dir}/{zipfilename}"
                    if data_transfer_type == "HTTP": #需要http请求下载
                        import wget
                        import urllib.request
                        try:
                            with urllib.request.urlopen(simu_data_zip_path, timeout=10) as response:
                                # headers = response.info()
                                content_length = int(response.headers['Content-Length'])
                            if content_length > 0:
                                wget.download(simu_data_zip_path, out=local_zipfile_path)
                            else:
                                raise Exception(f"[Actor, download_data_files]: zipfile={simu_data_zip_path} is empty!")
                        except Exception as e:
                            raise Exception(f"[Actor, download_data_files]: Download zipfile={simu_data_zip_path} from http link failed, because of {e}")
                    elif data_transfer_type == "S3": #需要从S3下载
                        from ols.taskMgr.utils.utils_oflserver import downloadS3File
                        isDownloadZipFile = downloadS3File(
                            manager_config = config.get("manager_config", ""),
                            local_file = local_zipfile_path,
                            taget_file = simu_data_zip_path
                        )
                        if isDownloadZipFile == False:
                           raise Exception(f"[Actor, download_data_files]: Download zipfile={simu_data_zip_path} from S3 failed")
                    elif data_transfer_type == "MINIO": #需要从MINIO下载
                        from ols.taskMgr.utils.utils_oflserver import downloadMinioFile
                        isDownloadZipFile = downloadMinioFile(
                            manager_config=config.get("manager_config", ""),
                            local_file=local_zipfile_path,
                            taget_file=simu_data_zip_path
                        )
                        if isDownloadZipFile == False:
                            raise Exception(
                                f"[Actor, download_data_files]: Download zipfile={simu_data_zip_path} from MINIO failed")

                # 解压下载的zip包
                tmp_taskid_data_path = f"{local_dir}/data"
                os.makedirs(tmp_taskid_data_path, exist_ok=True, mode=0o777)
                zip_file = zipfile.ZipFile(local_zipfile_path, "r")
                zip_file.extractall(path=tmp_taskid_data_path)
                zip_file.close()
                extract_path_final = tmp_taskid_data_path
                data_files_all = os.listdir(extract_path_final)
                data_subfolder_name = [file for file in data_files_all if "dataconfig.json" not in file] #去除掉dataconfig.json的影响

                # 更新本地下载后的数据地址
                if data_split_type == False: # 理论上只有一个文件夹
                    if len(data_subfolder_name) != 1:
                        raise AssertionError(f"[Actor, download_data_files]: length of data_subfolder_name({len(data_subfolder_name)})"
                                             f" is not equal to 1, data_split_type={data_split_type}")
                    simu_data_path = f"{extract_path_final}/{data_subfolder_name[0]}"
                    local_data_path_list = [simu_data_path] * len(self.split_index_list)
                else: # 理论上有多个子文件夹
                    if len(data_subfolder_name) != self.simulation_num:
                        raise AssertionError(f"[Actor, download_data_files]: length of data_subfolder_name({len(data_subfolder_name)})"
                                             f" is not equal to simulation_num, simulation_num={self.simulation_num}")
                    local_data_path_list = []
                    folder_name_list = sorted(data_subfolder_name)
                    for split_index in self.split_index_list:
                        local_data_path_list.append(
                            f"{extract_path_final}/{folder_name_list[split_index]}"
                        )

                # 如果有dataconfig.json
                if "dataconfig.json" in data_files_all:
                    with open(f"{extract_path_final}/dataconfig.json", "r") as f_dataconfig:
                        dataconfig = json.load(f_dataconfig)
                else:
                    dataconfig = {}

            else:
                # 如果不需要下载数据，则需要更新local_data_path_list
                local_data_path_list = []
                # 目前只支持FILE和MINIO不把数据全部下载，只进行个别读取/下载

                if data_transfer_type == "FILE":
                    data_files_all = os.listdir(simu_data_zip_path)
                    data_subfolder_name = [os.path.join(simu_data_zip_path, file) for file in data_files_all if "dataconfig.json" not in file]  # 去除掉dataconfig.json的影响

                # MINIO不下载
                elif data_transfer_type == "MINIO":
                    from ols.taskMgr.utils.utils_oflserver import listdirMinioFile
                    data_files_all = listdirMinioFile(
                        manager_config=config.get("manager_config", ""),
                        prefix=f"{simu_data_zip_path}/"
                    )
                    data_subfolder_name = [file for file in data_files_all if "dataconfig.json" not in file]  # 去除掉dataconfig.json的影响

                else:
                    raise NotImplementedError

                # 更新本地下载后的数据地址
                if data_split_type == False:  # 理论上只有一个文件夹
                    if len(data_subfolder_name) != 1:
                        raise AssertionError(
                            f"[Actor, download_data_files]: length of data_subfolder_name({len(data_subfolder_name)})"
                            f" is not equal to 1, data_split_type={data_split_type}")
                    simu_data_path = f"{data_subfolder_name[0]}"
                    local_data_path_list = [simu_data_path] * len(self.split_index_list)
                else:  # 理论上有多个子文件夹
                    if len(data_subfolder_name) != self.simulation_num:
                        raise AssertionError(
                            f"[Actor, download_data_files]: length of data_subfolder_name({len(data_subfolder_name)})"
                            f" is not equal to simulation_num, simulation_num={self.simulation_num}")
                    folder_name_list = sorted(data_subfolder_name)
                    for split_index in self.split_index_list:
                        local_data_path_list.append(
                            f"{folder_name_list[split_index]}"
                        )

                #TODO
                dataconfig = {}

            self.run_params.update({
                "download_data": {
                    "local_data_path_list": local_data_path_list,
                    "dataconfig": dataconfig
                }
            })
            return True

        except Exception as e:
            logging.error(f"[run]: download_data_files failed, because of {e}")
            return False

    def download_model_files(self, local_model_dir, model, task_id, current_round, config):
        use_model = model.get("use_model", False)
        if use_model == False:  # 如果不需要用到模型
            return True
        # 如果需要下载模型
        model_for_train = model.get("model_for_train", False)
        model_transfer_type = model.get("model_transfer_type", "S3")
        model_path = model.get("model_path", "")
        model_update_style = model.get("model_update_style", "{task_id}_{current_round}_result_model.mnn")
        model_path_new = model_update_style.replace("{task_id}", task_id).replace("{current_round}", str(current_round))

        try:
            _, model_ext = os.path.splitext(model_path)

            os.makedirs(local_model_dir, exist_ok=True, mode=0o777)
            local_model_path = f"{local_model_dir}/{task_id}_{current_round}_model{model_ext}"

            # 更新模型下载路径
            if model_for_train == True and current_round > 0:
                # 如果为训练且当前轮次大于0，则为model_path_new
                model_download_path = model_path_new
            else:
                # 其他情况均为model_path
                model_download_path = model_path

            # 下载模型
            if model_transfer_type == "HTTP":  # 需要http请求下载
                import wget
                import urllib.request
                try:
                    with urllib.request.urlopen(model_download_path, timeout=10) as response:
                        # headers = response.info()
                        content_length = int(response.headers['Content-Length'])
                    if content_length > 0:
                        wget.download(model_download_path, out=local_model_path)
                    else:
                        raise Exception(f"[Actor, download_model_files]: model file={model_download_path} is empty!")
                except Exception as e:
                    raise Exception(
                        f"[Actor, download_model_files]: Download model file={model_download_path} from http link failed, because of {e}")
            elif model_transfer_type == "S3":  # 需要从S3下载
                from ols.taskMgr.utils.utils_oflserver import downloadS3File
                isDownloadModelFile = downloadS3File(
                    manager_config=config.get("manager_config", ""),
                    local_file=local_model_path,
                    taget_file=model_download_path
                )
                if isDownloadModelFile == False:
                    raise Exception(
                        f"[Actor, download_model_files]: Download model file={model_download_path} from S3 failed")
            elif model_transfer_type == "MINIO": # 需要从MINIO下载
                from ols.taskMgr.utils.utils_oflserver import downloadMinioFile
                isDownloadModelFile = downloadMinioFile(
                    manager_config=config.get("manager_config", ""),
                    local_file=local_model_path,
                    taget_file=model_download_path
                )
                if isDownloadModelFile == False:
                    raise Exception(
                        f"[Actor, download_model_files]: Download model file={model_download_path} from MINIO failed")

            # 更新本地模型地址
            self.run_params.update({
                "download_model": {
                    "local_model_path": local_model_path
                }
            })
            return True
        except Exception as e:
            logging.error(f"[run]: download_model_files failed, because of {e}")
            return False

    def download_files_for_actor(self, config, data, operator):
        '''
        每个actor下载数据文件和模型，而且只下载一次，避免多次重复下载
        '''
        ray_local_data_path = config.get("ray_local_data_path", "")
        # 下载并解压数据文件
        # 如果operator需要下载数据，则下载数据
        use_data = operator.get("use_data", False)
        if use_data == True:
            local_data_dir = f"{ray_local_data_path}/{self.task_id}/{self.actor_id}/data"
            is_download_data = self.download_data_files(
                local_dir = local_data_dir,
                config = config,
                data = data
            )
            if is_download_data == False:
                return False
        else:
            is_download_data = True

        # 下载模型文件
        local_model_dir = f"{ray_local_data_path}/{self.task_id}/{self.actor_id}/model"
        is_download_model = self.download_model_files(
            local_model_dir = local_model_dir,
            model = operator.get("model", {}),
            task_id = self.task_id,
            current_round = self.current_round,
            config = config
        )
        return is_download_data and is_download_model

    def construct_params_for_phones(self, actor_params):
        # 构造actor里每个仿真手机所需要的运行参数
        phone_params_list = []
        data = actor_params.get("data", {})
        operator = actor_params.get("operator", {})
        local_data_path_list = actor_params.get("download_data", {}).get("local_data_path_list", [])
        simulation_num = actor_params.get("simulation_num", 0)
        split_index_list = actor_params.get("split_index_list", [])
        ray_local_data_path = actor_params.get("config", {}).get("ray_local_data_path", "")
        for i, split_index in enumerate(split_index_list):
            # 调整data字段，替换并保留有用的部分
            phone_data = copy.deepcopy(data)
            removed_value = phone_data.pop("data_transfer_type", None)
            if local_data_path_list:
                phone_data_path = local_data_path_list[i]
            else:
                phone_data_path = ""
            phone_data.update({
                "data_path": phone_data_path,
                "dataconfig": actor_params.get("download_data", {}).get("dataconfig", {})
            })
            # 调整operator字段，替换并保留有用的部分
            phone_operator = copy.deepcopy(operator)
            model = phone_operator.get("model", {})
            model.update({
                "model_path": actor_params.get("download_model", {}).get("local_model_path", "")
            })
            removed_value = model.pop("model_for_train", None)
            removed_value = model.pop("model_transfer_type", None)
            removed_value = model.pop("model_update_style", None)
            operator_params = phone_operator.get("logical_simulation", {}).get("operator_params", "")
            phone_operator.update({
                "model": model,
                "operator_params": operator_params
            })
            removed_value = phone_operator.pop("logical_simulation", None)
            # 构造phone_params
            phone_params = {
                "config": actor_params.get("config", {}),
                "task_id": actor_params.get("task_id", ""),
                "current_round": actor_params.get("current_round", 0),
                "data": phone_data,
                "operator": phone_operator,
                "simulation_num": simulation_num,
                "split_index": split_index,
                "actor_save_dir": f"{ray_local_data_path}/{self.task_id}/{self.actor_id}",
                "actor_simulation_num": len(split_index_list)
            }
            phone_params_list.append(phone_params)
        return phone_params_list

    def loop_run(self, params_list):
        for params in params_list:
            # 单台手机执行
            res_status = self._single_compute_step(
                operator_name = self.operator_name,
                operator_script = self.operator_script,
                params = params,
                python_path=self.PYTHONPATH,
            )
            # 执行结果记录
            if res_status == 0:
                self.success_num += 1
            else:
                self.failed_num += 1

    def _single_compute_step(self, operator_name, operator_script, params, python_path=""):
        # print(f"params = \n{params}\n")
        # 将参数存成json文件
        split_index = params.get("split_index", 0)
        params_path = f"{self.actor_id}_{split_index}.json"
        with open(params_path, "w") as json_file:
            json.dump(params, json_file)
        # 构建运行命令
        run_script = f"python3 {operator_name}/{operator_script} --params " + params_path
        print(f"run_script = {run_script}")
        os.putenv("PYTHONPATH", python_path)
        res_status = os.system(run_script)
        if params:
            try:
                os.remove(params_path)
            except Exception as e:
                print(f"[_single_compute_step]: Delete params_path={params_path} failed!")
                logging.warning(f"[_single_compute_step]: Delete params_path={params_path} failed!")
        return res_status

    def delete_tmp_results(self):
        try:
            time.sleep(1)
            config = self.run_params.get("config", {})
            ray_local_data_path = config.get("ray_local_data_path", "")
            local_data_dir = f"{ray_local_data_path}/{self.task_id}/{self.actor_id}"
            if os.path.exists(local_data_dir):
                shutil.rmtree(local_data_dir)
            return True
        except Exception as e:
            logging.warning(f"[delete_tmp_results]: Delete local_data_dir failed!")
            return False

    def run(self, run_params):
        ####### params #######
        # "config": config_params,
        # "data": data_params,
        # "operator": operator,
        # "device_type": device,
        # "simulation_num": simulation_num
        # "split_index_list": split_index_list

        ### 获取Actor的运行参数
        self.get_params(run_params)

        ### 下载并解压数据文件，以及算子涉及的模型文件
        is_download_files = self.download_files_for_actor(
            config = run_params.get("config", {}),
            data = run_params.get("data", {}),
            operator = run_params.get("operator", {})
        )
        # print(f"is_download_files = {is_download_files}")
        # print(f"after download_files, self.run_params = \n{self.run_params}\n")
        if is_download_files == False:
            # 下载数据文件和模型文件失败的话，则直接返回失败信息
            res_num = {
                "data_name": run_params.get("data", {}).get("name", ""),
                "success": 0,
                "failed": len(self.split_index_list)
            }
            logging.error(f"[run]: Actor run failed, because of download_files failed")
            ### 删除本地下载和解压的文件夹
            del_result = self.delete_tmp_results()  # 删除本地下载和解压的文件夹
            return res_num

        ### 调整给每台手机分配对应的参数
        phone_params_list = self.construct_params_for_phones(actor_params=self.run_params)
        # print(f"phone_params_list = \n{phone_params_list}\n")

        ### 循环执行（模拟）多台手机
        self.loop_run(params_list=phone_params_list)

        ### 删除本地下载和解压的文件夹
        del_result = self.delete_tmp_results()  # 删除本地下载和解压的文件夹

        # 返回状态结果
        res_num = {
            "data_name": run_params.get("data", {}).get("name", ""),
            "success": self.success_num,
            "failed": self.failed_num
        }
        return res_num

def generate_outbound_service(outbound_service_jsonstring):
    outbound_service_json = None
    try:
        outbound_service_json = json.loads(outbound_service_jsonstring)
        is_json_style = True
    except:
        is_json_style = False

    if is_json_style:
        pulsar_client_json = outbound_service_json.get("pulsar_client", {})
        if pulsar_client_json:
            pulsar_client = deviceflow_pb2.PulsarClient(
                url = pulsar_client_json.get("url", ""),
                topic = pulsar_client_json.get("topic", ""),
            )
        else:
            pulsar_client = deviceflow_pb2.PulsarClient()

        websocket_json = outbound_service_json.get("websocket", {})
        if websocket_json:
            websocket = deviceflow_pb2.Websocket(
                url = websocket_json.get("url", "")
            )
        else:
            websocket = deviceflow_pb2.Websocket()

        outbound_service = deviceflow_pb2.OutboundService(
            pulsar_client=pulsar_client,
            websocket=websocket
        )

    else:
        outbound_service = deviceflow_pb2.OutboundService()

    return outbound_service


