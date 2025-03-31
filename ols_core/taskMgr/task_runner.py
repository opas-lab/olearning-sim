import os
import time
import json
import uuid
import grpc
import shutil

from ray.job_submission import JobSubmissionClient
from ols.proto import taskService_pb2 as taskService__pb2
from ols.proto import phoneMgr_pb2_grpc
from ols.taskMgr.utils.utils import taskconfig2json
from ols.taskMgr.utils.utils import TaskTableRepo, write_to_db
from ols.taskMgr.utils.utils_runner import HybridOptimizer, JobSubmitter, DeviceflowResgister
from ols.taskMgr.utils.utils_runner import generateRunTaskFile, get_operator_code, json2deviceconfig, fix_device_task_json
from ols.simu_log import Logger

import sqlalchemy
from sqlalchemy import MetaData, Table, insert

# filepath
FILEPATH = os.path.abspath(os.path.dirname(__file__))
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '..')) # ols所在文件夹的绝对路径
# task_table_path
TASK_TABLE_PATH = OLSPATH + "/config/taskmgr_table.yaml"

# log
logger = Logger()
logger.set_logger(log_yaml=os.path.abspath(os.path.join(FILEPATH, '..'))+"/config/repo_log.yaml")

class TaskRunner:
    def __init__(self, task):
        self._task = task
        self._job_id = None
        try:
            self._task_repo = TaskTableRepo(yaml_path=TASK_TABLE_PATH)
        except Exception as e:
            logger.error(task_id="", system_name="TaskMgr", module_name="task_manager",
                         message=f"[__init__]: Initiate self._task_repo failed, because of {e}")
            raise e

    def submit_rayjob(self, task_json, raycluster=""):
        task_id = task_json.get("task_id", "")
        # 判断是否有逻辑仿真任务
        entrypoint = "python3 run_task.py --task " + "\'" + json.dumps(task_json) + "\'"
        # 创建本地用于提交任务的临时文件夹working_dir（需要把代码文件复制/解压复制到临时文件夹）
        currentPath = os.path.abspath(os.path.dirname(__file__))
        parentPath = os.path.abspath(os.path.join(currentPath, '..'))
        codePath = os.path.abspath(os.path.join(parentPath, '../..'))
        working_dir = codePath + f"/tmp/tmp_{str(uuid.uuid4())}"
        try:  # 可能有文件夹权限问题
            os.makedirs(working_dir, exist_ok=True, mode=0o777)
        except Exception as e:
            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_runner",
                         message=f"[submit]: makedirs working_dir of task_id={task_id} failed, because of {e}")
            return False

        operators = task_json.get("operatorflow", {}).get("operators", [])
        is_get_code = get_operator_code(
            operators=operators,
            working_dir=working_dir)
        if is_get_code == False:
            return False
        is_generate_run_task = generateRunTaskFile(working_dir=working_dir)
        if is_generate_run_task == False:
            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_runner",
                         message=f"[submit]: generateRunTaskFile of task_id={task_id} failed.")
            return False
        try:  # 可能submit_job不成功
            client = JobSubmissionClient(raycluster)
            job_id = client.submit_job(
                entrypoint=entrypoint,
                runtime_env={
                    "working_dir": working_dir
                }
            )
            self._job_id = job_id
        except Exception as e:
            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_runner",
                         message=f"[submit]: submit_job of task_id={task_id} failed, because of {e}")
            return False
        try:  # 可能删除临时文件夹缺少权限
            time.sleep(1)
            shutil.rmtree(working_dir)
        except Exception as e:
            logger.info(task_id=self._task.taskID.taskID, system_name="TaskMgr", module_name="task_runner",
                        message=f"[submit]: shutil.rmtree working_dir={working_dir} failed, because of {e}")
        return True

    def submit_phonejob(self, task_json, phonemgr=""):
        task_id = task_json.get("task_id", "")

        # 针对outbound_service的定制化修改
        task_json = fix_device_task_json(task_json)

        phone_taskconfig = json2deviceconfig(jsonstring=json.dumps(task_json))

        # print(f"phone_taskconfig = \n{phone_taskconfig}")
        # raise NotImplementedError

        task_id = self._task.taskID.taskID
        try:
            with grpc.insecure_channel(phonemgr) as channel:
                stub = phoneMgr_pb2_grpc.TaskManagerStub(channel)
                response = stub.submitTask(phone_taskconfig)
            if response.isSuccess == True:
                return True
            else:
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_runner",
                             message=f"[submit]: submit task={task_id} in phonemgr failed.")
                return False
        except Exception as e:
            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_runner",
                         message=f"[submit]: submit task={task_id} in phonemgr failed, because of {e}")
            return False

    def submit(self, raycluster="", phonemgr="", deviceflow_config=""):
        try:
            task_json_all = taskconfig2json(self._task)
        except Exception as e:
            logger.error(task_id=self._task.taskID.taskID, system_name="TaskMgr", module_name="task_runner",
                         message=f"[submit]: taskconfig transforms to json failed, task = {self._task}, because of {e}")
            return False

        # 根据混合异构资源任务，划分异构资源的机次分配数，必要时需要对数据进行划分
        hybrid_optimizer = HybridOptimizer(task_json=task_json_all)
        target_data_list = hybrid_optimizer.fix_data_parameters() #涉及机次数自动分配
        split_data_flag = hybrid_optimizer.hybrid_split_data(
            task_id = self._task.taskID.taskID,
            data_list = target_data_list
        )
        if split_data_flag == False:
            logger.error(task_id=self._task.taskID.taskID, system_name="TaskMgr", module_name="task_runner",
                         message=f"[submit]: split data from hybrid resource setting failed, "
                                 f"task_id = {self._task.taskID.taskID}")
            return False

        # 任务参数整合，逻辑仿真和真机仿真
        logical_jobsubmitter = JobSubmitter(job_type="logical_simulation")
        logical_jobsubmitter.integrate_submission_info(
            task_json = task_json_all,
            data_list = target_data_list
        )
        device_jobsubmitter = JobSubmitter(job_type="device_simulation")
        device_jobsubmitter.integrate_submission_info(
            task_json=task_json_all,
            data_list=target_data_list
        )

        # print(f"logical_jobsubmitter._needs_submission = {logical_jobsubmitter._needs_submission}")
        # print(f"logical_jobsubmitter._submission_info = \n{json.dumps(logical_jobsubmitter._submission_info)}\n")
        # print(f"device_jobsubmitter._needs_submission = {device_jobsubmitter._needs_submission}")
        # print(f"device_jobsubmitter._submission_info = \n{json.dumps(device_jobsubmitter._submission_info)}\n")

        # 调用梯度中间站注册服务
        deviceflow_register = DeviceflowResgister(deviceflow_config=deviceflow_config)
        use_deviceflow = deviceflow_register.check_use_deviceflow(task_json=task_json_all)
        if use_deviceflow:
            deviceflow_register_flag = deviceflow_register.register_task(
                task_id = self._task.taskID.taskID,
                logical_simulation = logical_jobsubmitter._needs_submission,
                device_simulation = device_jobsubmitter._needs_submission
            )
            if deviceflow_register_flag == False:
                logger.error(task_id=self._task.taskID.taskID, system_name="TaskMgr", module_name="task_runner",
                             message=f"[submit]: deviceflow register failed!")
                return False

        # 逻辑仿真提交任务
        if logical_jobsubmitter._needs_submission:
            logical_submit_flag = self.submit_rayjob(
                task_json = logical_jobsubmitter._submission_info,
                raycluster = raycluster
            )
            # 需要往数据库写入逻辑仿真target的logical_target
            task_json = logical_jobsubmitter._submission_info
            task_id = task_json.get("task_id", "")
            data_list = task_json.get("target", {}).get("data", [])
            target_data_list = []
            for data in data_list:
                target_data_dict = {
                    "name": data.get("name", ""),
                    "simulation_target": data.get("simulation_target", {})
                }
                target_data_list.append(target_data_dict)
            logical_target = {"logical_target": target_data_list}
            if not self._task_repo.set_item_value(task_id=task_id, item="logical_target",
                                                  value=json.dumps(logical_target)):
                logical_submit_flag = False
        else:
            logical_submit_flag = True
        if logical_submit_flag == False:
            return False

        # 真机仿真提交任务
        if device_jobsubmitter._needs_submission: #判断是否有真机任务
            device_submit_flag = self.submit_phonejob(
                task_json = device_jobsubmitter._submission_info,
                phonemgr = phonemgr
            )
            # 需要往数据库写入真机仿真target的logical_target
            task_json = device_jobsubmitter._submission_info
            task_id = task_json.get("task_id", "")
            data_list = task_json.get("target", {}).get("data", [])
            target_data_list = []
            for data in data_list:
                target_data_dict = {
                    "name": data.get("name", ""),
                    "simulation_target": data.get("simulation_target", {})
                }
                target_data_list.append(target_data_dict)
            device_target = {"device_target": target_data_list}
            if not self._task_repo.set_item_value(task_id=task_id, item="device_target",
                                                  value=json.dumps(device_target)):
                device_submit_flag = False
        else:
            device_submit_flag = True

        # 临时写表，为了展示
        try:
            self.temp_write_table(task_json=task_json_all)
        except Exception as e:
            print(f"[temp_write_table]: execute failed, because of {e}")

        return logical_submit_flag and device_submit_flag

    def temp_write_table(self, task_json):
        data_list = task_json["target"]["data"]
        total_target = {"High": 0, "Middle":0, "Low": 0}
        for data in data_list:
            devices = data["total_simulation"]["devices"]
            nums = data["total_simulation"]["nums"]
            for i in range(len(devices)):
                try:
                    total_target[devices[i]] = total_target[devices[i]] + nums[i]
                except:
                    print(f"devices[i] = {devices[i]}, nums[i] = {nums[i]}")
                    pass
        task_id = task_json["task_id"]
        # sql_engine = sqlalchemy.create_engine(
        #     f"mysql+pymysql://root:privacy123456@10.114.63.132:3306/virtually_phone_DB"
        # )
        sql_engine = sqlalchemy.create_engine(
            f"mysql+pymysql://root:privacy123456@10.52.52.96:3306/virtually_phone_DB"
        )
        sql_table = Table("task_target", MetaData(), autoload=True, autoload_with=sql_engine)
        delete_stmt = sql_table.delete().where(sql_table.c.task_id == task_id)
        sql_engine.execute(delete_stmt)
        item = {"task_id": [task_id]}
        if total_target["High"]>0:
            item.update({"high": [total_target["High"]]})
        if total_target["Middle"]>0:
            item.update({"middle": [total_target["Middle"]]})
        if total_target["Low"]>0:
            item.update({"low": [total_target["Low"]]})
        insert_stmt = insert(sql_table).values(**item)
        sql_engine.execute(insert_stmt)