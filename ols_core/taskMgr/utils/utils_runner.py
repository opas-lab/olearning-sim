import os
import json
import uuid
import copy
import yaml
import shutil
import zipfile
import pulp as lp
from ols.proto import taskService_pb2
from ols.simu_log import Logger


import grpc
from ols.proto import deviceflow_pb2, deviceflow_pb2_grpc

# filepath
FILEPATH = os.path.abspath(os.path.dirname(__file__))   # utils_runner.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '../..')) # ols所在文件夹的绝对路径
# log
logger = Logger()
logger.set_logger(log_yaml = OLSPATH + "/config/repo_log.yaml")

class HybridOptimizer:
    def __init__(self, task_json, data_folder="data_split"):
        self._task_json = task_json
        self._data_folder = data_folder
        self._data_json = dict()

    def fix_data_parameters(self):
        target_data_list = []
        # 获取可能有用的参数
        origin_data_list = self._task_json.get("target",{}).get("data", [])
        computation_unit_logical = self._task_json.get("logical_simulation", {}).get("computation_unit", [])
        resource_request_logical = self._task_json.get("logical_simulation",{}).get("resource_request", [])
        resource_request_device = self._task_json.get("device_simulation", {}).get("resource_request", [])
        # 逐个数据进行异构资源分配
        for data_index, origin_data in enumerate(origin_data_list):
            optimization_flag = origin_data.get("allocation", {}).get("optimization", False)
            if optimization_flag:
                # 机次数优化分配
                target_data = self.hybrid_optimization(
                    data_json = origin_data,
                    computation_unit = computation_unit_logical,
                    logical_resource = resource_request_logical[data_index],
                    device_resource = resource_request_device[data_index]
                )
            else:
                # 机次数优化分配由用户指定
                target_data = self.hybrid_setting_by_user(data_json=origin_data)
            target_data_list.append(target_data)
        return target_data_list

    def hybrid_setting_by_user(self, data_json):
        data_name = data_json.get("name", "")
        data_path = data_json.get("data_path", "")
        data_folder = os.path.dirname(data_path)
        # 计算逻辑仿真
        logical_num = sum(data_json.get("allocation",{}).get("logical_simulation", []))
        # device_num = sum(data_json.get("allocation",{}).get("device_simulation", [])) + \
        #             sum(data_json.get("allocation",{}).get("running_response", {}).get("nums", []))
        device_num = sum(data_json.get("allocation", {}).get("device_simulation", []))
        # 设置路径名
        data_path_logical = f"{data_folder}/{self._data_folder}/{data_name}_logical.zip"
        data_path_device = f"{data_folder}/{self._data_folder}/{data_name}_device.zip"
        # 更新
        target_data = data_json
        # 根据不同的任务目标分配数规定参数更新
        if logical_num == 0 and device_num == 0:
            target_data.update({"data_path_logical": "", "data_path_device": ""})
        elif logical_num != 0 and device_num == 0:
            target_data.update({"data_path_logical": data_path, "data_path_device": ""})
        elif logical_num == 0 and device_num != 0:
            target_data.update({"data_path_logical": "", "data_path_device": data_path})
        else:
            target_data.update({
                "data_path_logical": data_path_logical,
                "data_path_device": data_path_device
            })
        # print(f"target_data = {target_data}")
        return target_data

    # TODO
    def hybrid_optimization(self, data_json, computation_unit, logical_resource, device_resource):
        '''
        根据任务数和申请的资源数进行机次优化分配
        allocation的logical_simulation, device_simulation都需要赋值
        '''

        # 构造用于计算data_dict
        target_devices = data_json.get("total_simulation", {}).get("devices", [])
        running_response = data_json.get("allocation", {}).get("running_response", {})
        running_response_nums = rearrange_by_devices(
            target_devices, running_response.get("devices", []), running_response.get("nums", []), 0
        )
        computation_unit_setting_origin = rearrange_by_devices(
            target_devices, computation_unit.get("devices", []), computation_unit.get("setting", []), {"num_cpus": 0}
        )
        computation_unit_setting = [cu["num_cpus"] for cu in computation_unit_setting_origin]
        logical_num_request = rearrange_by_devices(
            target_devices, logical_resource.get("devices", []), logical_resource.get("num_request", []), 0
        )
        device_num_request = rearrange_by_devices(
            target_devices, device_resource.get("devices", []), device_resource.get("num_request", []), 0
        )

        # 机次数分配
        if sum(logical_num_request) == 0:
            # 没有申请逻辑仿真资源
            nclass_all = len(data_json.get("total_simulation", {}).get("nums", []))
            allocation_logical_simulation = [0] * nclass_all
            allocation_device_simulation = data_json.get("total_simulation", {}).get("nums", [])

        elif sum(device_num_request) == 0:
            # 没有申请真机仿真资源
            nclass_all = len(data_json.get("total_simulation", {}).get("nums", []))
            allocation_logical_simulation = data_json.get("total_simulation", {}).get("nums", [])
            allocation_device_simulation = [0] * nclass_all

        else:
            data_dict = {
                "N": data_json.get("total_simulation", {}).get("nums", []),
                "f": logical_num_request,
                "k": computation_unit_setting,
                "m": device_num_request,
                "q": running_response_nums
            }
            allocation_logical_simulation, allocation_device_simulation = auto_allocation_hybrid_task(data_dict)

        # print(f"allocation_logical_simulation = {allocation_logical_simulation}, allocation_device_simulation = {allocation_device_simulation}")

        data_json["allocation"]["logical_simulation"] = allocation_logical_simulation
        data_json["allocation"]["device_simulation"] = allocation_device_simulation

        # 更新参数
        data_name = data_json.get("name", "")
        data_path = data_json.get("data_path", "")
        data_folder = os.path.dirname(data_path)
        # 计算逻辑仿真
        logical_num = sum(data_json.get("allocation", {}).get("logical_simulation", []))
        device_num = sum(data_json.get("allocation", {}).get("device_simulation", [])) + \
                     sum(data_json.get("allocation", {}).get("running_response", {}).get("nums", []))
        # 设置路径名
        data_path_logical = f"{data_folder}/{self._data_folder}/{data_name}_logical.zip"
        data_path_device = f"{data_folder}/{self._data_folder}/{data_name}_device.zip"
        # 更新
        target_data = data_json
        # 根据不同的任务目标分配数规定参数更新
        if logical_num == 0 and device_num == 0:
            target_data.update({"data_path_logical": "", "data_path_device": ""})
        elif logical_num != 0 and device_num == 0:
            target_data.update({"data_path_logical": data_path, "data_path_device": ""})
        elif logical_num == 0 and device_num != 0:
            target_data.update({"data_path_logical": "", "data_path_device": data_path})
        else:
            target_data.update({
                "data_path_logical": data_path_logical,
                "data_path_device": data_path_device
            })
        # print(f"target_data = {target_data}")
        return target_data

    def hybrid_split_data(self, task_id, data_list):
        split_id = str(uuid.uuid4())
        for data in data_list:
            data_path_logical = data.get("data_path_logical", "")
            data_path_device = data.get("data_path_device", "")
            if data_path_logical != "" and data_path_device != "":
                hybrid_data_splitter = HybridDataSplitter(
                    task_id = task_id,
                    task_type = data.get("task_type", "classification"),
                    split_id = split_id
                )
                split_flag = hybrid_data_splitter.split(data=data)
                if split_flag == False:
                    return False
        return True

class HybridDataSplitter:
    def __init__(self, task_id="", task_type="classification", split_id=""):
        self._task_id = task_id
        self._split_id = split_id
        self._task_type = task_type
        self._tmp_data_folder = os.path.abspath(os.path.join(OLSPATH, '../..')) + f"/tmp/tmpdata_{self._split_id}"

    # TODO
    def split(self, data):
        '''
        不同task_type的数据划分方式
        '''
        if self._task_type == "classification":
            split_flag = self.split_data_classification(data=data)
            shutil.rmtree(self._tmp_data_folder) # 删除临时数据文件夹
            return split_flag

    def split_data_classification(self, data,
                                  train_split=True, test_split=False,
                                  random_state=42):
        import pandas as pd
        from sklearn.model_selection import train_test_split
        data_name = data.get("name", "")
        data_path = data.get("data_path", "")
        data_split_type = data.get("data_split_type", False)

        # 计算异构资源数据份数
        logical_num = sum(data.get("allocation",{}).get("logical_simulation", []))
        device_num = sum(data.get("allocation",{}).get("device_simulation", [])) + \
                    sum(data.get("allocation",{}).get("running_response", {}).get("nums", []))

        # 下载数据文件
        is_download = self.download_data(data)
        if is_download == False:
            logger.error(task_id=f"{self._task_id}", system_name="TaskMgr", module_name="task_runner",
                         message=f"[split_data_classification]: download data from data_path = {data_path} failed")
            return False
        # 解压数据文件
        is_extract = self.extract_data_from_zip(data)
        if is_extract == False:
            logger.error(task_id=f"{self._task_id}", system_name="TaskMgr", module_name="task_runner",
                         message=f"[split_data_classification]: extract data_file = {data_name} failed")
            return False
        # 获取文件夹中子文件夹的数量
        data_folder = f"{self._tmp_data_folder}/{data_name}"
        data_subfolder_name = [file for file in os.listdir(data_folder) if "dataconfig.json" not in file]
        # 数据划分
        # 如果需要进行数据自动划分
        if data_split_type == False:
            if len(data_subfolder_name) != 1:
                logger.error(task_id=f"{self._task_id}", system_name="TaskMgr", module_name="task_runner",
                             message=f"[split_data_classification]: data_split_type = {data_split_type}, data_subfolder_name={data_subfolder_name}"
                                     f"len(data_subfolder_name) is not equal to 1")
                return False
            ### 逻辑仿真 ###
            data_logical_path = f"{self._tmp_data_folder}/{data_name}_logical"
            os.makedirs(f"{data_logical_path}/{data_subfolder_name[0]}", exist_ok=True, mode=0o777)
            shutil.copy2(f"{data_folder}/dataconfig.json", data_logical_path)
            # 处理训练集文件
            if train_split:
                train_features_path = f"{data_folder}/{data_subfolder_name[0]}/train_features.csv"
                train_data = pd.read_csv(train_features_path)
                train_labels_path = f"{data_folder}/{data_subfolder_name[0]}/train_labels.csv"
                train_labels = pd.read_csv(train_labels_path)
                # 划分
                device_ratio = device_num / (logical_num + device_num)
                # device_ratio != 0 and device_ratio != 1
                logical_train_features, device_train_features, logical_train_labels, device_train_labels = \
                    train_test_split(train_data, train_labels, test_size=device_ratio, random_state=random_state)
                # 保存
                logical_train_features.to_csv(f"{data_logical_path}/{data_subfolder_name[0]}/train_features.csv",
                                              index=False)
                logical_train_labels.to_csv(f"{data_logical_path}/{data_subfolder_name[0]}/train_labels.csv",
                                            index=False)
            else:
                shutil.copy2(f"{data_folder}/{data_subfolder_name[0]}/train_features.csv",
                             f"{data_logical_path}/{data_subfolder_name[0]}")
                shutil.copy2(f"{data_folder}/{data_subfolder_name[0]}/train_labels.csv",
                             f"{data_logical_path}/{data_subfolder_name[0]}")
            # 处理测试集文件
            if test_split:
                test_features_path = f"{data_folder}/{data_subfolder_name[0]}/test_features.csv"
                test_data = pd.read_csv(test_features_path)
                test_labels_path = f"{data_folder}/{data_subfolder_name[0]}/test_labels.csv"
                test_labels = pd.read_csv(test_labels_path)
                # 划分
                device_ratio = device_num / (logical_num + device_num)
                # device_ratio != 0 and device_ratio != 1
                logical_test_features, device_test_features, logical_test_labels, device_test_labels = \
                    train_test_split(test_data, test_labels, test_size=device_ratio, random_state=random_state)
                # 保存
                logical_test_features.to_csv(f"{data_logical_path}/{data_subfolder_name[0]}/test_features.csv",
                                             index=False)
                logical_test_labels.to_csv(f"{data_logical_path}/{data_subfolder_name[0]}/test_labels.csv",
                                           index=False)
            else:
                shutil.copy2(f"{data_folder}/{data_subfolder_name[0]}/test_features.csv",
                             f"{data_logical_path}/{data_subfolder_name[0]}")
                shutil.copy2(f"{data_folder}/{data_subfolder_name[0]}/test_labels.csv",
                             f"{data_logical_path}/{data_subfolder_name[0]}")
            # 打包
            logical_data_files_all = os.listdir(data_logical_path)
            logical_data_subfolder_name = [file for file in logical_data_files_all if "dataconfig.json" not in file]
            # 创建一个 zip 文件
            with zipfile.ZipFile(f"{data_logical_path}.zip", 'w', zipfile.ZIP_DEFLATED) as my_zip:
                # 将每个文件添加到 zip 包里
                for foldername, subfolders, filenames in os.walk(f"{data_logical_path}/{logical_data_subfolder_name[0]}"):
                    my_zip.write(foldername, f"{logical_data_subfolder_name[0]}")
                    for filename in filenames:
                        file_path = os.path.join(foldername, filename)
                        my_zip.write(file_path, f"{logical_data_subfolder_name[0]}/{filename}")
                my_zip.write(f"{data_logical_path}/dataconfig.json", "dataconfig.json")

            ### 真机仿真 ###
            data_device_path = f"{self._tmp_data_folder}/{data_name}_device"
            os.makedirs(f"{data_device_path}/{data_subfolder_name[0]}", exist_ok=True, mode=0o777)
            shutil.copy2(f"{data_folder}/dataconfig.json", data_device_path)
            # 处理训练集文件
            if train_split:
                # device_ratio != 0 and device_ratio != 1:
                device_train_features.to_csv(f"{data_device_path}/{data_subfolder_name[0]}/train_features.csv", index=False)
                device_train_labels.to_csv(f"{data_device_path}/{data_subfolder_name[0]}/train_labels.csv", index=False)
            else:
                shutil.copy2(f"{data_folder}/{data_subfolder_name[0]}/train_features.csv",
                             f"{data_device_path}/{data_subfolder_name[0]}")
                shutil.copy2(f"{data_folder}/{data_subfolder_name[0]}/train_labels.csv",
                             f"{data_device_path}/{data_subfolder_name[0]}")
            # 处理测试集文件
            if test_split:
                # device_ratio != 0 and device_ratio != 1:
                device_test_features.to_csv(f"{data_device_path}/{data_subfolder_name[0]}/test_features.csv", index=False)
                device_test_labels.to_csv(f"{data_device_path}/{data_subfolder_name[0]}/test_labels.csv", index=False)
            else:
                shutil.copy2(f"{data_folder}/{data_subfolder_name[0]}/test_features.csv",
                             f"{data_device_path}/{data_subfolder_name[0]}")
                shutil.copy2(f"{data_folder}/{data_subfolder_name[0]}/test_labels.csv",
                             f"{data_device_path}/{data_subfolder_name[0]}")
            # 打包
            device_data_files_all = os.listdir(data_device_path)
            device_data_subfolder_name = [file for file in device_data_files_all if "dataconfig.json" not in file]
            # 创建一个 zip 文件
            with zipfile.ZipFile(f"{data_device_path}.zip", 'w', zipfile.ZIP_DEFLATED) as my_zip:
                # 将每个文件添加到 zip 包里
                for foldername, subfolders, filenames in os.walk(
                        f"{data_device_path}/{device_data_subfolder_name[0]}"):
                    my_zip.write(foldername, f"{device_data_subfolder_name[0]}")
                    for filename in filenames:
                        file_path = os.path.join(foldername, filename)
                        my_zip.write(file_path, f"{device_data_subfolder_name[0]}/{filename}")
                my_zip.write(f"{data_device_path}/dataconfig.json", "dataconfig.json")
        # TODO: 未测试
        # 如果用户已经将数据进行划分
        else:
            if len(data_subfolder_name) != logical_num + device_num:
                logger.error(task_id=f"{self._task_id}", system_name="TaskMgr", module_name="task_runner",
                             message=f"[split_data_classification]: data_split_type = {data_split_type}, data_subfolder_name={data_subfolder_name}"
                                     f"len(data_subfolder_name) is not equal to logical_num + device_num")
                return False
            ### 逻辑仿真 ###
            data_logical_path = f"{self._tmp_data_folder}/{data_name}_logical"
            os.makedirs(f"{data_logical_path}", exist_ok=True, mode=0o777)
            shutil.copy2(f"{data_folder}/dataconfig.json", data_logical_path)
            for folder_ind in range(logical_num):
                data_subfolder_instance = data_subfolder_name[folder_ind]
                shutil.copytree(f"{data_folder}/{data_subfolder_instance}",
                                f"{data_logical_path}/{data_subfolder_instance}")
            # 打包, 创建一个 zip 文件
            with zipfile.ZipFile(f"{data_logical_path}.zip", 'w', zipfile.ZIP_DEFLATED) as my_zip:
                for folder_ind in range(logical_num):
                    data_subfolder_instance = data_subfolder_name[folder_ind]
                    # 将每个文件添加到 zip 包里
                    for foldername, subfolders, filenames in os.walk(
                            f"{data_logical_path}/{data_subfolder_instance}"):
                        my_zip.write(foldername, f"{data_subfolder_instance}")
                        for filename in filenames:
                            file_path = os.path.join(foldername, filename)
                            my_zip.write(file_path, f"{data_subfolder_instance}/{filename}")
                my_zip.write(f"{data_logical_path}/dataconfig.json", "dataconfig.json")

            ### 真机仿真 ###
            data_device_path = f"{self._tmp_data_folder}/{data_name}_device"
            os.makedirs(f"{data_device_path}", exist_ok=True, mode=0o777)
            shutil.copy2(f"{data_folder}/dataconfig.json", data_device_path)
            for folder_ind in range(logical_num, logical_num + device_num):
                data_subfolder_instance = data_subfolder_name[folder_ind]
                shutil.copytree(f"{data_folder}/{data_subfolder_instance}",
                                f"{data_device_path}/{data_subfolder_instance}")
            # 打包, 创建一个 zip 文件
            with zipfile.ZipFile(f"{data_device_path}.zip", 'w', zipfile.ZIP_DEFLATED) as my_zip:
                for folder_ind in range(logical_num, logical_num + device_num):
                    data_subfolder_instance = data_subfolder_name[folder_ind]
                    # 将每个文件添加到 zip 包里
                    for foldername, subfolders, filenames in os.walk(
                            f"{data_device_path}/{data_subfolder_instance}"):
                        my_zip.write(foldername, f"{data_subfolder_instance}")
                        for filename in filenames:
                            file_path = os.path.join(foldername, filename)
                            my_zip.write(file_path, f"{data_subfolder_instance}/{filename}")
                my_zip.write(f"{data_device_path}/dataconfig.json", "dataconfig.json")
        # 上传文件
        is_upload = self.upload_data(data)
        if is_upload == False:
            logger.error(task_id=f"{self._task_id}", system_name="TaskMgr", module_name="task_runner",
                         message=f"[split_data_classification]: updata split data failed")
            return False

    def download_data(self, data):
        os.makedirs(self._tmp_data_folder, exist_ok=True, mode=0o777)
        data_path = data.get("data_path", "")
        data_transfer_type = data.get("data_transfer_type", "S3")
        zip_file_name = os.path.basename(data_path)
        local_data_zip = f"{self._tmp_data_folder}/{zip_file_name}"
        if data_transfer_type == "HTTP":
            import wget
            import urllib.request
            try:
                with urllib.request.urlopen(data_path, timeout=10) as response:
                    # headers = response.info()
                    content_length = int(response.headers['Content-Length'])
                if content_length > 0:
                    wget.download(data_path, out=local_data_zip)
                else:
                    logger.error(task_id=f"{self._task_id}", system_name="TaskMgr", module_name="task_runner",
                                 message=f"[download_data]: data_path={data_path} is empty!")
                    return False
            except Exception as e:
                return False
        elif data_transfer_type == "S3":
            from ols.taskMgr.utils.utils_oflserver import downloadS3File
            isDownloadZipFile = downloadS3File(
                manager_config = OLSPATH + "/config/manager_config.yaml",
                local_file=local_data_zip,
                taget_file=data_path
            )
            if isDownloadZipFile == False:
                return False
        elif data_transfer_type == "MINIO":
            from ols.taskMgr.utils.utils_oflserver import downloadMinioFile
            isDownloadZipFile = downloadMinioFile(
                manager_config=OLSPATH + "/config/manager_config.yaml",
                local_file=local_data_zip,
                taget_file=data_path
            )
            if isDownloadZipFile == False:
                return False

        return True

    def extract_data_from_zip(self, data):
        data_name = data.get("name", "")
        data_path = data.get("data_path", "")
        zip_file_name = os.path.basename(data_path)
        local_data_zip = f"{self._tmp_data_folder}/{zip_file_name}"
        extract_data_path = f"{self._tmp_data_folder}/{data_name}"
        try:
            os.makedirs(extract_data_path, exist_ok=True, mode=0o777)
            zip_file = zipfile.ZipFile(local_data_zip, "r")
            zip_file.extractall(path=extract_data_path)
            zip_file.close()
        except Exception as e:
            logger.error(task_id=f"{self._task_id}", system_name="TaskMgr", module_name="task_runner",
                         message=f"[extract_data_from_zip]: extract local_data_zip={local_data_zip} failed, because of {e}")
            return False
        return True

    def upload_data(self, data):
        data_name = data.get("name", "")
        data_transfer_type = data.get("data_transfer_type", "S3")
        if data_transfer_type == "S3":
            from ols.taskMgr.utils.utils_oflserver import uploadS3File
            isUploadLogical = uploadS3File(
                manager_config = OLSPATH + "/config/manager_config.yaml",
                local_file = f"{self._tmp_data_folder}/{data_name}_logical.zip",
                taget_file = data.get("data_path_logical", "")
            )
            isUploadPhone = uploadS3File(
                manager_config = OLSPATH + "/config/manager_config.yaml",
                local_file = f"{self._tmp_data_folder}/{data_name}_device.zip",
                taget_file = data.get("data_path_device", "")
            )
            return isUploadLogical and isUploadPhone

        elif data_transfer_type == "MINIO":
            from ols.taskMgr.utils.utils_oflserver import uploadMinioFile
            isUploadLogical = uploadMinioFile(
                manager_config=OLSPATH + "/config/manager_config.yaml",
                local_file=f"{self._tmp_data_folder}/{data_name}_logical.zip",
                taget_file=data.get("data_path_logical", "")
            )
            isUploadPhone = uploadMinioFile(
                manager_config=OLSPATH + "/config/manager_config.yaml",
                local_file=f"{self._tmp_data_folder}/{data_name}_device.zip",
                taget_file=data.get("data_path_device", "")
            )
            return isUploadLogical and isUploadPhone

        else:
            # TODO: HTTP模式下的数据上传
            raise NotImplementedError

class JobSubmitter:
    def __init__(self, job_type):
        self._job_type = job_type
        self._needs_submission = False
        self._submission_info = dict()

    def integrate_submission_info(self, task_json, data_list):
        if self._job_type == "logical_simulation":
            need_submission, logical_simulation_info = \
                self.assemble_info_logical_simulation(task_json=task_json, data_list=data_list)
            self._needs_submission = need_submission
            self._submission_info = logical_simulation_info
        elif self._job_type == "device_simulation":
            need_submission, device_simulation_info = \
                self.assemble_info_device_simulation(task_json=task_json, data_list=data_list)
            self._needs_submission = need_submission
            self._submission_info = device_simulation_info
        else:
            raise NotImplementedError

    def assemble_info_logical_simulation(self, task_json, data_list):
        need_submission = False
        logical_simulation_info = {}
        # 根据data_list判断是否需要进行逻辑仿真（判断任务目标是否为0，有没有对应的数据路径）
        for data in data_list:
            logical_num = sum(data.get("allocation", {}).get("logical_simulation", []))
            data_path_logical = data.get("data_path_logical", "")
            # if logical_num > 0 and data_path_logical != "":
            if logical_num > 0:
                need_submission = True
                break
        if need_submission == False:
            return False, {}
        # 需要进行逻辑仿真，则需要拼接一下对应的数据
        submit_data_list = []
        for data in data_list:
            submit_data = copy.deepcopy(data)
            # 更新逻辑仿真数据路径
            submit_data.update({"data_path": data.get("data_path_logical", "")})
            removed_value = submit_data.pop("data_path_logical", None)
            removed_value = submit_data.pop("data_path_device", None)
            # 更新逻辑仿真机次数目标
            submit_data.update({
                "simulation_target": {
                    "devices": data.get("total_simulation", {}).get("devices", []),
                    "nums": data.get("allocation", {}).get("logical_simulation", []),
                }
            })
            removed_value = submit_data.pop("total_simulation", None)
            removed_value = submit_data.pop("allocation", None)
            submit_data_list.append(submit_data)
        # 需要对算子流进行拼接
        operatorflow_json = task_json.get("operatorflow", {})
        flow_setting_json = operatorflow_json.get("flow_setting", {})
        operators_json = operatorflow_json.get("operators", [])
        submit_operatorflow = {}
        submit_operators = []
        for operator_json in operators_json:
            submit_operator_json = copy.deepcopy(operator_json)
            removed_value = submit_operator_json.pop("device_simulation", None)
            submit_operators.append(submit_operator_json)
        submit_operatorflow.update({
            "flow_setting": {
                "round": flow_setting_json.get("round", 0),
                "start": {
                    "logical_simulation": flow_setting_json.get("start", {}).get("logical_simulation", {})
                },
                "stop": {
                    "logical_simulation": flow_setting_json.get("stop", {}).get("logical_simulation", {})
                }
            },
            "operators": submit_operators
        })
        # 最后拼一下逻辑仿真所需要的参数
        logical_simulation_info.update({
            "user_id": task_json.get("user_id", ""),
            "task_id": task_json.get("task_id", ""),
            "target": {
                "data": submit_data_list
            },
            "operatorflow": submit_operatorflow,
            "logical_simulation": task_json.get("logical_simulation", {})
        })
        return need_submission, logical_simulation_info

    def assemble_info_device_simulation(self, task_json, data_list):
        need_submission = False
        device_simulation_info = {}
        # 根据data_list判断是否需要进行真机仿真（判断任务目标是否为0，有没有对应的数据路径）
        for data in data_list:
            device_num = sum(data.get("allocation", {}).get("device_simulation", [])) + \
                         sum(data.get("allocation", {}).get("running_response", {}).get("nums", []))
            data_path_device = data.get("data_path_device", "")
            # if device_num > 0 and data_path_device != "":
            if device_num > 0:
                need_submission = True
                break
        if need_submission == False:
            return False, {}
        # 需要进行真机，则需要拼接一下对应的数据
        submit_data_list = []
        for data in data_list:
            submit_data = copy.deepcopy(data)
            # 更新逻辑仿真数据路径
            submit_data.update({"data_path": data.get("data_path_device", "")})
            removed_value = submit_data.pop("data_path_logical", None)
            removed_value = submit_data.pop("data_path_device", None)
            # 更新逻辑仿真机次数目标
            submit_data.update({
                "simulation_target": {
                    "devices": data.get("total_simulation", {}).get("devices", []),
                    "nums": data.get("allocation", {}).get("device_simulation", []),
                },
                "running_response": data.get("allocation", {}).get("running_response", {})
            })
            removed_value = submit_data.pop("total_simulation", None)
            removed_value = submit_data.pop("allocation", None)
            submit_data_list.append(submit_data)
        # 需要对算子流进行拼接
        operatorflow_json = task_json.get("operatorflow", {})
        flow_setting_json = operatorflow_json.get("flow_setting", {})
        operators_json = operatorflow_json.get("operators", [])
        submit_operatorflow = {}
        submit_operators = []
        for operator_json in operators_json:
            submit_operator_json = copy.deepcopy(operator_json)
            removed_value = submit_operator_json.pop("logical_simulation", None)
            submit_operators.append(submit_operator_json)
        submit_operatorflow.update({
            "flow_setting": {
                "round": flow_setting_json.get("round", 0),
                "start": {
                    "device_simulation": flow_setting_json.get("start", {}).get("device_simulation", {})
                },
                "stop": {
                    "device_simulation": flow_setting_json.get("stop", {}).get("device_simulation", {})
                }
            },
            "operators": submit_operators
        })
        # 最后拼一下逻辑仿真所需要的参数
        device_simulation_info.update({
            "user_id": task_json.get("user_id", ""),
            "task_id": task_json.get("task_id", ""),
            "target": {
                "data": submit_data_list
            },
            "operatorflow": submit_operatorflow,
            "device_simulation": task_json.get("device_simulation", {})
        })
        return need_submission, device_simulation_info

class DeviceflowResgister:
    def __init__(self, deviceflow_config=""):
        with open(deviceflow_config, 'r') as file:
            config = yaml.load(file, Loader=yaml.FullLoader)
        self._grpc_service_address = config["grpc_service_address"]

    def check_use_deviceflow(self, task_json):
        use_deviceflow = False
        operatorflow_json = task_json.get("operatorflow", {})
        operators_json = operatorflow_json.get("operators", [])
        for operator_json in operators_json:
            operator_use_deviceflow = operator_json.get("operation_behavior_controller", {}).get("use_gradient_house", False)
            if operator_use_deviceflow == True:
                use_deviceflow = True
                break
        return use_deviceflow

    def register_task(self, task_id, logical_simulation, device_simulation):
        total_compute_resources = []
        if logical_simulation == True:
            total_compute_resources.append("logical_simulation")
        if device_simulation == True:
            total_compute_resources.append("device_simulation")
        # 调用梯度中间站RegisterTask接口进行注册
        try:
            with grpc.insecure_channel(self._grpc_service_address) as channel:
                deviceflow_stub = deviceflow_pb2_grpc.TaskOperatorOrientedDeviceFlowStub(channel)

                response = deviceflow_stub.RegisterTask(
                    deviceflow_pb2.RegisterRequest(
                        task_id=task_id,
                        total_compute_resources=total_compute_resources
                    )
                )
                if response.is_success:
                    return True
                else:
                    return False
        except Exception as e:
            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_runner",
                         message=f"[DeviceflowResgister]: register_task failed, because of {e}")
            return False


def generateRunTaskFile(working_dir=""):
    try:
        currentPath = os.path.abspath(os.path.dirname(__file__))
        parentPath = os.path.abspath(os.path.join(currentPath, '..'))
        save_path = f"{working_dir}/run_task.py"
        shutil.copy2(f"{parentPath}/run_task.py", save_path)
        return True
    except:
        return False

def get_operator_code(operators=[], working_dir=""):
    for operator in operators:
        operator_name = operator.get("name", "")
        operator_code_path = operator.get("logical_simulation",{}).get("operator_code_path", "")
        operator_entry_file = operator.get("logical_simulation",{}).get("operator_entry_file", "")

        file_transfer_type_Name = operator.get("logical_simulation",{}).get("operator_transfer_type", "")

        # 如果代码在本地，且operator_code_path存在，且是路径dir的形式，则直接复制到对应目录
        if file_transfer_type_Name == "FILE" and os.path.isdir(os.path.abspath(operator_code_path)):
            working_dir_operator = f"{working_dir}/{operator_name}"
            try:
                shutil.copytree(operator_code_path, working_dir_operator)
                if operator_entry_file not in os.listdir(working_dir_operator): # 校验判断算子脚本是否存在
                    return False
            except Exception as e:
                logger.error(task_id="", system_name="TaskMgr", module_name="task_runner",
                             message=f"[get_operator_code]: file_transfer_type={file_transfer_type_Name}, operator_code_path={operator_code_path}, copytree failed, because of {e}")
                return False
        else:  # 除了上述情况外，代码文件均为zip包形式
            zip_file_name = os.path.basename(operator_code_path)
            zip_folder_name, _ = os.path.splitext(zip_file_name)
            operator_zip_path = f"{working_dir}/" + zip_file_name

            # 把zip包复制或下载到working_dir
            if file_transfer_type_Name == "FILE":
                try:
                    shutil.copy2(operator_code_path, operator_zip_path)
                except:
                    logger.error(task_id="", system_name="TaskMgr", module_name="task_runner",
                                 message=f"[get_operator_code]: file_transfer_type={file_transfer_type_Name}, operator_code_path={operator_code_path}, copy to working_dir failed")
                    return False

            if file_transfer_type_Name == "HTTP":
                import wget
                import urllib.request
                try:
                    with urllib.request.urlopen(operator_code_path, timeout=10) as response:
                        # headers = response.info()
                        content_length = int(response.headers['Content-Length'])
                    if content_length > 0:
                        wget.download(operator_code_path, out=operator_zip_path)
                    else:
                        logger.error(task_id="", system_name="TaskMgr", module_name="task_runner",
                                     message=f"[get_operator_code]: file_transfer_type={file_transfer_type_Name}, operator_code_path = {operator_code_path} is empty!")
                        return False
                except Exception as e:
                    logger.error(task_id="", system_name="TaskMgr", module_name="task_runner",
                                 message=f"[get_operator_code]: file_transfer_type={file_transfer_type_Name}, download {operator_code_path} failed, because of {e}")
                    return False

            if file_transfer_type_Name == "S3":
                from ols.taskMgr.utils.utils_oflserver import downloadS3File
                current_file_path = os.path.abspath(os.path.dirname(__file__))
                ols_path = os.path.abspath(os.path.join(current_file_path, '../..'))
                isDownloadZipFile = downloadS3File(
                    manager_config=ols_path + "/config/manager_config.yaml",
                    local_file=operator_zip_path,
                    taget_file=operator_code_path
                )
                if isDownloadZipFile == False:
                    logger.error(task_id="", system_name="TaskMgr", module_name="task_runner",
                                 message=f"[get_operator_code]: file_transfer_type={file_transfer_type_Name}, download {operator_code_path} failed")
                    return False

            if file_transfer_type_Name == "MINIO":
                from ols.taskMgr.utils.utils_oflserver import downloadMinioFile
                current_file_path = os.path.abspath(os.path.dirname(__file__))
                ols_path = os.path.abspath(os.path.join(current_file_path, '../..'))
                isDownloadZipFile = downloadMinioFile(
                    manager_config=ols_path + "/config/manager_config.yaml",
                    local_file=operator_zip_path,
                    taget_file=operator_code_path
                )
                if isDownloadZipFile == False:
                    logger.error(task_id="", system_name="TaskMgr", module_name="task_runner",
                                 message=f"[get_operator_code]: file_transfer_type={file_transfer_type_Name}, download {operator_code_path} failed")
                    return False

            try:
                zip_file = zipfile.ZipFile(operator_zip_path, "r")
                zip_file.extractall(path=working_dir)
                zip_file.close()
                os.remove(operator_zip_path)  # 删除复制的代码zip包文件

                # 考虑operator名和operator_code_path的解压文件名不相同的情况，需要搬移一下文件
                if zip_folder_name != operator_name:
                    os.rename(f"{working_dir}/{zip_folder_name}", f"{working_dir}/{operator_name}")

                # 校验判断算子脚本是否存在
                working_dir_operator = f"{working_dir}/{operator_name}"
                if operator_entry_file not in os.listdir(working_dir_operator): # 校验判断算子脚本是否存在
                    return False

            except Exception as e:
                logger.error(task_id="", system_name="TaskMgr", module_name="task_runner",
                             message=f"[get_operator_code]: zip_file.extractall of os.remove failed, because of {e}")
                return False
    return True

def json2deviceconfig(jsonstring):
    jsondata = json.loads(jsonstring)

    # target
    target_json = jsondata.get("target", {})
    data_json_list = target_json.get("data", [])
    target_data_list = []
    for data_index, data_json in enumerate(data_json_list):
        simulation_target_json = data_json.get("simulation_target", {})
        simulation_target = taskService_pb2.DeviceTotalSimulation(
            devices = simulation_target_json.get("devices", []),
            nums = simulation_target_json.get("nums", [])
        )
        running_response_json = data_json.get("running_response", {})
        running_response = taskService_pb2.RunningResponse(
            deviceRunningResponse = running_response_json.get("devices", []),
            numRunningResponse = running_response_json.get("nums", [])
        )
        data_transfer_type_str = data_json.get("data_transfer_type", "S3")
        target_data = taskService_pb2.DeviceTargetData(
            dataName = data_json.get("name", f"data_{data_index}"),
            dataPath = data_json.get("data_path", ""),
            dataSplitType = data_json.get("data_split_type", False),
            dataTransferType = eval(f"taskService_pb2.FileTransferType.{data_transfer_type_str}"),
            taskType = data_json.get("task_type", ""),
            deviceTotalSimulation = simulation_target,
            runningResponse = running_response
        )
        target_data_list.append(target_data)

    # operatorflow
    operatorflow_json = jsondata.get("operatorflow", {})
    flow_setting_json = operatorflow_json.get("flow_setting", {})
    operator_list_json = operatorflow_json.get("operators", [])
    # operatorflow - flow_setting - start
    start_strategy_json = flow_setting_json.get("start", {})
    start_strategy_device_simulation_json = start_strategy_json.get("device_simulation", {})
    start_strategy_device_simulation = taskService_pb2.StrategyCondition(
        strategyCondition = start_strategy_device_simulation_json.get("strategy", ""),
        waitInterval = start_strategy_device_simulation_json.get("wait_interval", 0),
        totalTimeout = start_strategy_device_simulation_json.get("total_timeout", 0)
    )
    # operatorflow - flow_setting - stop
    stop_strategy_json = flow_setting_json.get("stop", {})
    stop_strategy_device_simulation_json = stop_strategy_json.get("device_simulation", {})
    stop_strategy_device_simulation = taskService_pb2.StrategyCondition(
        strategyCondition = stop_strategy_device_simulation_json.get("strategy", ""),
        waitInterval = stop_strategy_device_simulation_json.get("wait_interval", 0),
        totalTimeout = stop_strategy_device_simulation_json.get("total_timeout", 0)
    )
    flow_setting = taskService_pb2.DeviceFlowSetting(
        round = flow_setting_json.get("round", 0),
        deviceStartCondition = start_strategy_device_simulation,
        deviceStopCondition = stop_strategy_device_simulation
    )

    # operatorflow - operator
    operator_list = []
    for operator_json in operator_list_json:
        # operation_behavior_controller
        operation_behavior_controller_json = operator_json.get("operation_behavior_controller", {})
        operation_behavior_controller = taskService_pb2.OperationBehaviorController(
            useController = operation_behavior_controller_json.get("use_gradient_house", False),
            strategyBehaviorController = operation_behavior_controller_json.get("strategy_gradient_house", ""),
            outboundService = operation_behavior_controller_json.get("outbound_service", "")
        )
        # model
        model_json = operator_json.get("model", {})
        model_transfer_type_str = model_json.get("model_transfer_type", "S3")
        model = taskService_pb2.Model(
            useModel = model_json.get("use_model", False),
            modelForTrain = model_json.get("model_for_train", False),
            modelTransferType = eval(f"taskService_pb2.FileTransferType.{model_transfer_type_str}"),
            modelPath = model_json.get("model_path", ""),
            modelUpdateStyle = model_json.get("model_update_style", "")
        )
        # device_simulation_operator_info
        device_simulation_operator_info_json = operator_json.get("device_simulation", {})
        operator_transfer_type_str = device_simulation_operator_info_json.get("operator_transfer_type", "S3")
        device_simulation_operator_info = taskService_pb2.OperatorSimulationInfo(
            operatorTransferType = eval(f"taskService_pb2.FileTransferType.{operator_transfer_type_str}"),
            operatorCodePath = device_simulation_operator_info_json.get("operator_code_path", ""),
            operatorEntryFile = device_simulation_operator_info_json.get("operator_entry_file", ""),
            operatorParams = device_simulation_operator_info_json.get("operator_params", "")
        )
        operator = taskService_pb2.DeviceOperator(
            name = operator_json.get("name", ""),
            operationBehaviorController = operation_behavior_controller,
            input = operator_json.get("input", []),
            useData = operator_json.get("use_data", False),
            model = model,
            deviceSimulationOperatorInfo = device_simulation_operator_info
        )
        operator_list.append(operator)
    operatorflow = taskService_pb2.DeviceOperatorFlow(
        deviceFlowSetting = flow_setting,
        deviceOperator = operator_list
    )

    # device_simulation
    device_simulation_json = jsondata.get("device_simulation", {})
    resource_request_list_json = device_simulation_json.get("resource_request", {})
    resource_request_device_simulation_list = []
    for resource_request_json in resource_request_list_json:
        resource_request_device_simulation = taskService_pb2.ResourceRequest(
            dataNameResourceRequest = resource_request_json.get("name", ""),
            deviceResourceRequest = resource_request_json.get("devices", []),
            numResourceRequest = resource_request_json.get("num_request", []),
        )
        resource_request_device_simulation_list.append(resource_request_device_simulation)

    phone_taskconfig = taskService_pb2.DeviceTaskConfig(
        userID = jsondata.get("user_id", ""),
        taskID = jsondata.get("task_id", ""),
        deviceTargetData = target_data_list,
        deviceOperatorFlow = operatorflow,
        resourceRequestDeviceSimulation = resource_request_device_simulation_list
    )
    return phone_taskconfig


def fix_device_task_json(jsondata):
    operatorflow_json = jsondata.get("operatorflow", {})
    operator_list_json = operatorflow_json.get("operators", [])
    for operator_ind, operator_json in enumerate(operator_list_json):
        operation_behavior_controller_json = operator_json.get("operation_behavior_controller", {})
        outbound_service = operation_behavior_controller_json.get("outbound_service", "")
        if outbound_service != "":
            try:
                outbound_service_json = json.loads(outbound_service)
                outbound_service_url = outbound_service_json.get("websocket", {}).get("url", "")
                jsondata["operatorflow"]["operators"][operator_ind]["operation_behavior_controller"]["outbound_service"] = outbound_service_url
            except:
                outbound_service = ""

    # 特定在使用真机参数时做task_id的替换
    task_id = jsondata.get("task_id", "")
    for operator_ind, operator_json in enumerate(operator_list_json):
        device_operator_params = operator_json.get("device_simulation", {}).get("operator_params", "")
        if device_operator_params != "":
            device_operator_json = json.loads(device_operator_params)
            origin_task_id = device_operator_json.get("environment", {}).get("task_id", "")
            if origin_task_id != "":
                device_operator_json["environment"]["task_id"] = task_id
            device_operator_params = json.dumps(device_operator_json)
            jsondata["operatorflow"]["operators"][operator_ind]["device_simulation"][
                "operator_params"] = device_operator_params
            
    return jsondata


def rearrange_by_devices(devices, key_list, value_list, default_value):
    value_dict = {k: v for k, v in zip(key_list, value_list)}
    return [value_dict.get(device, default_value) for device in devices]

def auto_allocation_hybrid_task(data_dict):
    # params
    params_alpha = [3.5]     # logical单轮耗时
    params_beta = [0.14]     # 真机单轮耗时
    params_lambda = [8.808]  # 真机固定启动时间

    # 构造参数
    nclass_all = len(data_dict["N"])
    allocation_logical_simulation = [0] * nclass_all
    allocation_device_simulation = [0] * nclass_all
    abort_index = []
    for i in range(nclass_all):
        if data_dict.get("f", [])[i] == 0:
            allocation_device_simulation[i] = data_dict.get("N", [])[i]
            abort_index.append(i)
        elif data_dict.get("m", [])[i] == 0:
            allocation_logical_simulation[i] = data_dict.get("N", [])[i]
            abort_index.append(i)

    remain_index = []
    N, f, k, m, q = [], [], [], [], []
    for i in range(nclass_all):
        if i not in abort_index:
            remain_index.append(i)
            N.append(data_dict.get("N", [])[i])
            f.append(data_dict.get("f", [])[i])
            k.append(data_dict.get("k", [])[i])
            m.append(data_dict.get("m", [])[i])
            q.append(data_dict.get("q", [])[i])

    data_dict.update({"N": N, "f": f, "k": k, "m": m, "q": q})
    data_dict.update({
        "alpha": params_alpha * len(remain_index),
        "beta": params_beta * len(remain_index),
        "lambda": params_lambda * len(remain_index)
    })

    # print(f"data_dict = {data_dict}")

    nclass = len(data_dict["N"])

    # 创建问题实例
    prob = lp.LpProblem("Integer_LP", lp.LpMinimize)

    # 定义变量
    x_vars = {i: lp.LpVariable(f"X_{i}", lowBound=0, upBound=data_dict["N"][i] - data_dict["q"][i], cat='Integer') for i in range(nclass)}
    ceil_vars_logical = lp.LpVariable.dicts("CeilLogical", range(nclass), lowBound=0, cat='Integer')
    ceil_vars_phone = lp.LpVariable.dicts("CeilPhone", range(nclass), lowBound=0, cat='Integer')

    # 新变量 z 用于表示目标函数中最大耗时
    z = lp.LpVariable("z", lowBound=0)
    for i in range(nclass):
        # 对于 ceil(x_vars[i] / data_dict["f"][i] * data_dict["k"][i]) 操作
        prod_logical = x_vars[i] * data_dict["k"][i]
        prob += ceil_vars_logical[i] >= prod_logical / data_dict["f"][i]
        prob += ceil_vars_logical[i] <= (prod_logical + (data_dict["f"][i] - 1)) / data_dict["f"][i]

        time_logical = ceil_vars_logical[i] * data_dict["alpha"][i]

        # 对于 ceil((data_dict["N"][i] - x_vars[i]) / data_dict["m"][i]) 操作
        prob += ceil_vars_phone[i] >= (data_dict["N"][i] - data_dict["q"][i] - x_vars[i]) / data_dict["m"][i]
        prob += ceil_vars_phone[i] <= ((data_dict["N"][i] - data_dict["q"][i] - x_vars[i]) + (data_dict["m"][i] - 1)) / \
                data_dict["m"][i]

        time_phone = ceil_vars_phone[i] * data_dict["beta"][i] + data_dict["lambda"][i]

        prob += z >= time_logical
        prob += z >= time_phone

    # 设置目标函数
    prob += z

    # 求解问题
    prob.solve(lp.PULP_CBC_CMD(msg=False))

    # 获取结果
    x_optimal = [int(v.varValue) for v in prob.variables() if 'X' in v.name]

    # 整理结果
    for i in range(len(x_optimal)):
        allocation_logical_simulation[remain_index[i]] = x_optimal[i]
        allocation_device_simulation[remain_index[i]] = int(data_dict.get("N", [])[remain_index[i]] - x_optimal[i])

    return allocation_logical_simulation, allocation_device_simulation