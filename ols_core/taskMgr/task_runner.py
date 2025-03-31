import os
import time
import json
import uuid
import grpc
import shutil

from ray.job_submission import JobSubmissionClient
from ols.proto import taskService_pb2 as taskService__pb2
from ols.proto import phoneMgr_pb2_grpc
from ols.taskMgr.utils.utils import generateRunTaskFile, taskconfig2json, get_operator_code
from ols.simu_log import Logger

# filepath
FILEPATH = os.path.abspath(os.path.dirname(__file__))
# log
logger = Logger()
logger.set_logger(log_yaml=os.path.abspath(os.path.join(FILEPATH, '..'))+"/config/repo_log.yaml")

class TaskRunner:
    def __init__(self, task):
        self._task = task
        self._job_id = None

    def submit_rayjob(self, task_json, raycluster=""):
        task_id = self._task.taskID.taskID
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
        # 逐个operator遍历获得代码地址，复制或者下载
        file_transfer_type_str = task_json.get("file_transfer_type", "")
        try:
            file_transfer_type = eval(f"taskService__pb2.FileTransferType.{file_transfer_type_str}")
        except:
            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_runner",
                         message=f"[submit]: file_transfer_type={file_transfer_type_str}, should be in taskService__pb2.FileTransferType")
            return False
        operators = task_json.get("operators", [])
        is_get_code = get_operator_code(
            file_transfer_type=file_transfer_type,
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

    def submit_phonejob(self, task_device, phonemgr=""):
        task_id = self._task.taskID.taskID
        try:
            with grpc.insecure_channel(phonemgr) as channel:
                stub = phoneMgr_pb2_grpc.TaskManagerStub(channel)
                response = stub.submitTask(task_device)
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

    def submit(self, raycluster="", phonemgr=""):
        try:
            task_json_all = taskconfig2json(self._task)
        except Exception as e:
            logger.error(task_id=self._task.taskID.taskID, system_name="TaskMgr", module_name="task_runner",
                         message=f"[submit]: taskconfig transforms to json failed, task = {self._task}, because of {e}")
            return False

        if self._task.logicalSimulation.operators: #判断是否有逻辑仿真任务
            logical_submit_flag = self.submit_rayjob(
                task_json=task_json_all.get("logical_simulation", {}),
                raycluster=raycluster
            )
        else:
            logical_submit_flag = True
        if logical_submit_flag == False:
            return False

        if self._task.deviceSimulation.deviceRequirement: #判断是否有真机任务
            device_submit_flag = self.submit_phonejob(
                task_device=self._task.deviceSimulation,
                phonemgr=phonemgr
            )
        else:
            device_submit_flag = True

        return logical_submit_flag and device_submit_flag

