import os
import time
import json
import grpc
import threading
import configparser
from ray.job_submission import JobSubmissionClient
from ols.proto import taskService_pb2 as taskService__pb2
from ols.proto.taskService_pb2_grpc import TaskMgrServicer
from ols.proto import phoneMgr_pb2_grpc  # 导入真机侧的proto
from ols.taskMgr.task_queue import TaskQueue
from ols.taskMgr.task_scheduler import TaskScheduler
from ols.taskMgr.task_runner import TaskRunner
from ols.simu_log import Logger
from ols.taskMgr.utils.utils import TaskTableRepo, write_to_db
from ols.taskMgr.utils.utils import ValidateParameters, json2taskconfig, taskconfig2json, get_current_time

# filepath
FILEPATH = os.path.abspath(os.path.dirname(__file__))   # task_manager.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '..')) # ols所在文件夹的绝对路径
CONFIG_PATH = OLSPATH + "/config/config.conf"
# task_table_path
TASK_TABLE_PATH = OLSPATH + "/config/taskmgr_table.yaml"
# log
logger = Logger()
logger.set_logger(log_yaml=OLSPATH + "/config/repo_log.yaml")

# task status
class TaskStatus:
    SUCCEEDED = "SUCCEEDED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    MISSING = "MISSING"
    UNDONE = "UNDONE"
    QUEUED = "QUEUED"

class TaskManager(TaskMgrServicer):
    def __init__(self):
        self._running_flag = False
        self._config = dict()
        self._raydashboard = ""
        self._scheduler_sleep_time = 0
        try:
            self._task_repo = TaskTableRepo(yaml_path=TASK_TABLE_PATH)
        except Exception as e:
            logger.error(task_id="", system_name="TaskMgr", module_name="task_manager",
                         message=f"[__init__]: Initiate self._task_repo failed, because of {e}")
            raise e
        self._task_queue = TaskQueue()
        self._task_running = list()  # 占用资源的任务，使用task_table后，则不存入内存
        self.get_config_params()
        self.get_taskqueue_from_repo()
        ### run thread ###
        self._task_queue_thread = threading.Thread(target=self.run)
        self._task_queue_thread.start()
        self._releaseResource_thread = threading.Thread(target=self.releaseResource)
        self._releaseResource_thread.start()

    def get_taskqueue_from_repo(self):
        # 根据task_table更新任务队列self._task_queue
        queued_df = self._task_repo.get_data_by_item_values(item="task_status",
                                                            values=[TaskStatus.QUEUED])
        if queued_df.shape[0] > 1:
            queued_df = queued_df.sort_values("in_queue_time", ascending=True)  # 根据记录的入队列时间升序排序，无记录时间的任务自动排到了最后
        queued_task_id = []
        for queued_idx in range(queued_df.shape[0]):
            task_id = queued_df.iloc[[queued_idx]]["task_id"].item()
            resource_occupied = queued_df.iloc[[queued_idx]]["resource_occupied"].item()
            try:
                taskconfig_json = queued_df.iloc[[queued_idx]]["task_params"].item()
                taskconfig = json2taskconfig(taskconfig_json)
                if resource_occupied != 0: # 如果存在资源被占用但仍处于QUEUED状态的任务，需要释放资源
                    scheduler = TaskScheduler(resmgr=self._resmgr, phonemgr=self._phonemgr)
                    if scheduler.release(task=taskconfig):
                        write_to_db(task_repo=self._task_repo, task_id=task_id,
                                    item="resource_occupied", value=0,
                                    logger=logger, system_name="TaskMgr",
                                    module_name="task_manager", func_name="get_taskqueue_from_repo")
                        logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                    message=f"[get_taskqueue_from_repo]: Succeeded in releasing the resources of"
                                            f" {task_id} when the task resources were frozen and the submission failed.")
                    else:
                        logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                     message=f"[get_taskqueue_from_repo]: Failed to release the resource of"
                                             f" {task_id} when the task resources were frozen and the submission failed.")
                queued_task_id.append(task_id)
                self._task_queue.add(taskconfig)
            except Exception as e: #避免出现1个任务失败使得整个队列无法完成初始化
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                             message=f"[get_taskqueue_from_repo]: Add existed task_id={task_id} to queue failed,"
                                     f" because of {e}")
        # 打印重新进入task_queue的task_id
        if queued_task_id:
            logger.info(task_id="", system_name="TaskMgr", module_name="task_manager",
                        message=f"[get_taskqueue_from_repo]: These tasks have been reinserted into the task queue, task_ids = {queued_task_id}")
        else:
            logger.info(task_id="", system_name="TaskMgr", module_name="task_manager",
                        message=f"[get_taskqueue_from_repo]: No task have been reinserted into the task queue.")

        # 根据task_table更新self._task_running, 即正在占用cpu, mem资源的任务
        running_df = self._task_repo.get_data_by_item_values(item="resource_occupied", values=[1])
        running_task_id = []
        for running_idx in range(running_df.shape[0]):
            try:
                task_id = running_df.iloc[[running_idx]]["task_id"].item()
                self._task_running.append({
                    "task_id": task_id,
                    "job_id": running_df.iloc[[running_idx]]["job_id"].item(),
                    "task": json2taskconfig(running_df.iloc[[running_idx]]["task_params"].item())
                })
                running_task_id.append(task_id)
            except Exception as e:
                task_id = running_df.iloc[[running_idx]]["task_id"].item()
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                             message=f"[get_taskqueue_from_repo]: Append existed task_id={task_id} to"
                                     f" self._task_running failed, because of {e}")
        # 打印重新进入task_queue的task_id
        if running_task_id:
            logger.info(task_id="", system_name="TaskMgr", module_name="task_manager",
                        message=f"[get_taskqueue_from_repo]: These tasks have been reinserted into the"
                                f" task_running list, task_ids = {running_task_id}")
        else:
            logger.info(task_id="", system_name="TaskMgr", module_name="task_manager",
                        message=f"[get_taskqueue_from_repo]: No task have been reinserted into the"
                                f" task_running list.")


    def get_config_params(self):
        if os.path.exists(CONFIG_PATH):
            try:
                self._config = configparser.ConfigParser()
                self._config.read(CONFIG_PATH)
                self._raydashboard = f'http://{self._config.get("ray", "dashboard_ip")[1:-1]}:' \
                                     f'{self._config.get("ray", "dashboard_port")}'
                self._resmgr = f'{self._config.get("grpc", "resmgr_ip")[1:-1]}:' \
                               f'{self._config.get("grpc", "resmgr_port")}'
                self._phonemgr = f'{self._config.get("grpc", "phonemgr_ip")[1:-1]}:' \
                                 f'{self._config.get("grpc", "phonemgr_port")}'
                self._scheduler_sleep_time = int(self._config.get("taskMgr",
                                                                  "scheduler_sleep_time"))
                self._release_sleep_time = int(self._config.get("taskMgr",
                                                                "release_sleep_time"))
            except Exception as e:
                logger.error(task_id="", system_name="TaskMgr", module_name="task_manager",
                             message=f"[get_config_params]: Initiate from config.conf failed,"
                                     f" because of {e}")
                raise e
        else:
            logger.error(task_id="", system_name="TaskMgr", module_name="task_manager",
                        message=f"[get_config_params]: Can not find config file from {CONFIG_PATH}")
            raise Exception(f"Can not find config file from {CONFIG_PATH}")

    def submitTask(self, request, context):
        '''
        1. 参数校验：如果参数不满足约束条件，则返回错误，并记录错误。
        2. 查找接收到的task是否在数据库中，状态是否为UNDONE
            (1)如果task不在数据库中，返回错误，log记录：所提交的任务不在数据库中
            (2)如果task在数据库中，但状态不为UNDONE，返回错误，log记录：所提交的任务曾经存在，存在状态为{任务状态}
        3. 更改数据库中任务的状态为QUEUED，向数据库里记录任务参数
        4. 向任务队列添加任务
        5. 向数据库表中添加任务进入队列的时间
        '''
        # print(request)
        # 参数校验
        validation = ValidateParameters()
        if not validation.validate_task_parameters(request):
            return taskService__pb2.OperationStatus(is_success=False)
        # 获取task_id
        task_id = request.taskID.taskID
        # 检验task_id是否在数据库
        if not self._task_repo.check_task_in_database(task_id=task_id):
            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                         message=f"[submitTask]: task_id={task_id} is not in the task_table")
            return taskService__pb2.OperationStatus(is_success=False)
        task_status = self._task_repo.get_item_value(task_id=task_id, item="task_status")
        if task_status == None:
            return taskService__pb2.OperationStatus(is_success=False)
        # 检验task_status是否为UNDONE
        if task_status != TaskStatus.UNDONE:
            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                         message=f"[submitTask]: task_id={task_id} once existed, and the status is {task_status}")
            return taskService__pb2.OperationStatus(is_success=False)
        # 更改数据库中任务的状态为QUEUED，如果存在连接数据库失败等则返回失败
        if not self._task_repo.set_item_value(task_id=task_id, item="task_status", value=TaskStatus.QUEUED):
            return taskService__pb2.OperationStatus(is_success=False)
        # 向数据库里记录任务参数，如果存在连接数据库失败等则返回失败
        if not self._task_repo.set_item_value(task_id=task_id, item="task_params", value=json.dumps(taskconfig2json(request))):
            return taskService__pb2.OperationStatus(is_success=False)
        # 向任务队列添加任务
        self._task_queue.add(request)
        # 向数据库表中添加任务进入队列的时间
        if not self._task_repo.set_item_value(task_id=task_id, item="in_queue_time", value=get_current_time()):
            logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                        message=f"[submitTask]: Set in_queue_time of task_id={task_id} failed.") #只是在数据库中设定时间失败，但不影响执行
        logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                    message=f"[submitTask]: Add task = {task_id} to TaskQueue")
        return taskService__pb2.OperationStatus(is_success=True)

    def stopTask(self, request, context):
        try:
            task_id = request.taskID
        except Exception as e:
            logger.error(task_id="", system_name="TaskMgr", module_name="task_manager",
                        message=f"[stopTask]: Extract task_id from request = {request} failed, because of {e}")
            return taskService__pb2.OperationStatus(is_success=False)
        stop_status = self.stop_task(task_id=task_id)
        return taskService__pb2.OperationStatus(is_success=stop_status)

    def stop_task(self, task_id):
        '''
        如果资源被占用：
              则调用Ray的stop_job接口，调用真机侧的任务停止接口
        如果资源不被占用：
              如果在队列里：
                  直接在队列里删一遍任务，状态改为STOPPED
              否则：
                  logger.info记录任务状态，则返回停止成功
        '''
        resource_occupied = self._task_repo.get_item_value(task_id=task_id, item="resource_occupied")
        task_status = self._task_repo.get_item_value(task_id=task_id, item="task_status")
        if resource_occupied == 1:
            task_params_string = self._task_repo.get_item_value(task_id=task_id, item="task_params")
            if task_params_string is None:
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                             message=f"[stop_task]: Get task_params of task_id={task_id} is None, failed!")
                return False
            try:
                taskconfig = json2taskconfig(task_params_string)
            except:
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                             message=f"[stop_task]: task_params of task_id={task_id} does not meet submission requirements, failed!")
                return False
            stop_flag = self.stop_operation(
                task_id = task_id,
                task_status = task_status,
                taskconfig = taskconfig
            )
            if stop_flag == True:
                logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                            message=f"[stop_task]: Succeeded in stopping task_id={task_id} from Ray and PhoneMgr!")
            return stop_flag
        else:
            if self.checkTaskInTaskQueue(task_id):
                self.deleteTaskinTaskQueue(task_id)  # 从队列里先删一遍
                if self._task_repo.set_item_value(task_id=task_id, item="task_status", value=TaskStatus.STOPPED):
                    logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                message=f"[stop_task]: task_id={task_id} is removed from task_queue, stopTask success!")
                    return True
                else:
                    logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                 message=f"[stop_task]: Failed to set task_status={TaskStatus.STOPPED} of task_id={task_id} in task_repo.")
                    return False
            else:
                try:
                    eval(f"taskService__pb2.TaskStatusEnum.{task_status}")
                    logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                message=f"[stop_task]: Job status is {task_status}, no need to stop task of {task_id}.")
                    return True
                except:
                    logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                 message=f"[stop_task]: The task_status={task_status} of task_id={task_id} in task_repo is not permitted!")
                    return False

    def stop_operation(self, task_id, task_status, taskconfig):
        # 逻辑仿真
        if taskconfig.logicalSimulation.operators:
            # 逻辑仿真部分的停止
            job_id = self._task_repo.get_item_value(task_id=task_id, item="job_id")
            if job_id is None:  # 使用了逻辑仿真，但job_id为None,说明有问题
                logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                             message=f"[stopTask]: Job status is {task_status}, the job_id of task_id={task_id} is None during the logical_simulation, no need to stop!")
                logical_stop_flag = True
            else:
                try:
                    client = JobSubmissionClient(self._raydashboard)
                    client.stop_job(job_id=job_id)
                    logical_stop_flag = True
                    logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                message=f"[stop_operation]: Succeeded in stopping task_id={task_id} from Ray!")
                except Exception as e:
                    logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                 message=f"[stop_operation]: Job status is {task_status}, failed to stop the task of {task_id} in ray, because of {e}")
                    logical_stop_flag = False
        else: # 没有使用逻辑仿真
            logical_stop_flag = True
        # 真机侧
        if taskconfig.deviceSimulation.deviceRequirement: # 使用了真机侧
            # 真机侧部分的停止接口调用
            try:
                with grpc.insecure_channel(self._phonemgr) as channel:
                    stub = phoneMgr_pb2_grpc.TaskManagerStub(channel)
                    response_phonemgr = stub.stopDevice(taskService__pb2.TaskID(taskID=task_id))
                phonemgr_stop_flag = response_phonemgr.isSuccess
            except Exception as e:
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                             message=f"[stopTask]: Stop task_id={task_id} in PhoneMgr failed, because of {e}")
                phonemgr_stop_flag = False
        else: # 没有使用真机侧
            phonemgr_stop_flag = True
        stop_flag = logical_stop_flag and phonemgr_stop_flag
        return stop_flag

    def getTaskStatus(self, request, context):
        taskStatusMsg = self.get_task_status(task_id=request.taskID)
        return taskService__pb2.TaskStatus(taskStatus=eval(f"taskService__pb2.TaskStatusEnum.{taskStatusMsg}"))

    def get_task_status(self, task_id):
        '''
        检查task_id是否在repo里
            检查是否在队列里
                返回QUEUED
            如果不在
                如果是占用资源状态
                    //需要获取实时状态
                    （1）获取任务在ray集群中的状态
                        向ray集群获取任务状态
                        成功，则得到logical_task_status
                        失败，则查看状态
                            如果该任务是需要使用逻辑仿真的，则返回失败
                            否则，得到logical_task_status为成功
                    （2）获取任务在真机侧的状态
                        getDeviceTaskStatus(TaskID)，返回DeviceTaskResult(isFinished, successNum, failedNum)
                        成功，更新数据库，返回状态
                        失败，则查看状态
                            如果该任务是需要使用真机侧的，则返回失败
                            否则，得到device_task_status为成功
                    （3）综合判断两者状态
                        输入：task_id, ray集群状态, 真机侧状态
                        输出：最终状态，返回（只改状态，不停止）
                否则判断是否为允许状态
                    如果是，返回现有状态；
                    否则返回FAILED
        如果没有
            返回MISSING
        '''
        if self._task_repo.check_task_in_database(task_id=task_id):
            if self.checkTaskInTaskQueue(task_id):  # 在任务队列里没被分配
                return TaskStatus.QUEUED
            else:
                resource_occupied = self._task_repo.get_item_value(task_id=task_id, item="resource_occupied")
                if resource_occupied == 1:  # 如果资源被占用
                    task_params_string = self._task_repo.get_item_value(task_id=task_id, item="task_params")
                    if task_params_string is None:
                        logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                     message=f"[get_task_status]: Get task_params of task_id={task_id} is None, failed!")
                        return TaskStatus.FAILED
                    else:
                        try:
                            taskconfig = json2taskconfig(task_params_string)
                        except:
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[get_task_status]: task_params of task_id={task_id} does not meet submission requirements, failed!")
                            return TaskStatus.FAILED
                    # 仿真任务状态查询
                    if taskconfig.logicalSimulation.operators:
                        try:
                            job_id = self._task_repo.get_item_value(task_id=task_id, item="job_id")
                            client = JobSubmissionClient(self._raydashboard)
                            task_status = client.get_job_status(job_id=job_id)
                            logical_task_status = str(task_status)
                        except Exception as e:
                            return TaskStatus.FAILED
                    else:
                        logical_task_status = TaskStatus.SUCCEEDED
                    # 真机任务状态查询
                    if taskconfig.deviceSimulation.deviceRequirement:
                        try:
                            with grpc.insecure_channel(self._phonemgr) as channel:
                                stub = phoneMgr_pb2_grpc.TaskManagerStub(channel)
                                taskID = taskService__pb2.TaskID(taskID=task_id)
                                device_task_result = stub.getDeviceTaskStatus(taskID)
                            device_task_status = device_task_result.isFinished
                            write_to_db(task_repo=self._task_repo, task_id=task_id,
                                        item="phone_success_num", value=device_task_result.successNum,
                                        logger=logger, system_name="TaskMgr",
                                        module_name="task_manager", func_name="get_task_status")
                            write_to_db(task_repo=self._task_repo, task_id=task_id,
                                        item="phone_failed_num", value=device_task_result.failedNum,
                                        logger=logger, system_name="TaskMgr",
                                        module_name="task_manager", func_name="get_task_status")
                        except Exception as e:
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[get_task_status]: Get device_task_result from PhoneMgr Failed, because of {e}")
                            return TaskStatus.FAILED
                    else:
                        device_task_status = True
                    # 综合logical_task_status、device_task_status以及各任务允许的动态手机数
                    final_task_status = self.combine_task_status(
                        task_id=task_id, logical_task_status=logical_task_status, device_task_status=device_task_status
                    )
                    write_to_db(task_repo=self._task_repo, task_id=task_id,
                                item="task_status", value=final_task_status,
                                logger=logger, system_name="TaskMgr",
                                module_name="task_manager", func_name="get_task_status")
                    return final_task_status
                else:
                    taskStatusMsg = self._task_repo.get_item_value(task_id=task_id, item="task_status")
                    try:
                        eval(f"taskService__pb2.TaskStatusEnum.{taskStatusMsg}")
                        return taskStatusMsg
                    except:
                        logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                     message=f"[get_task_status]: The task_status={taskStatusMsg} of task_id={task_id} in task_repo is not permitted, change to FAILED!")
                        write_to_db(task_repo=self._task_repo, task_id=task_id,
                                    item="task_status", value=TaskStatus.FAILED,
                                    logger=logger, system_name="TaskMgr",
                                    module_name="task_manager", func_name="get_task_status")
                        return TaskStatus.FAILED
        else:  # 如果不在数据库中，则为MISSING
            return TaskStatus.MISSING

    def combine_task_status(self, task_id, logical_task_status, device_task_status):
        task_params_string = self._task_repo.get_item_value(task_id=task_id, item="task_params")
        if task_params_string is None:
            logger.error(task_id=task_id,
                         system_name="TaskMgr",
                         module_name="task_manager",
                         message=f"[get_task_status]: Get task_params of"
                                 f" task_id={task_id} is None, failed!")
            return TaskStatus.FAILED
        else:
            task_params = json.loads(task_params_string)
        logical_finished_flag, device_finished_flag = self.calculate_finished_condition(task_id=task_id,
                                                                                        task_params=task_params)
        # 总共有2*2*5*2=40种情况
        # SUCCEEDED: 共10种情况
        if logical_finished_flag and device_finished_flag:
            return TaskStatus.SUCCEEDED
        # STOPPED: 共2种情况
        if logical_finished_flag==False and logical_task_status==TaskStatus.STOPPED and device_task_status==True:
            return TaskStatus.STOPPED
        # FAILED: 共17种情况
        if logical_finished_flag==False and logical_task_status in [TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.STOPPED]:
            return TaskStatus.FAILED #12种, 但有2种和STOPPED重复
        if device_finished_flag==False and device_task_status==True:
            return TaskStatus.FAILED #10种情况，和以上有3种重叠，再其中有1种与STOPPED重复
        # RUNNING: 共11种情况
        return TaskStatus.RUNNING

    def calculate_finished_condition(self, task_id, task_params):
        # logical simulation
        logical_operator_list = task_params.get("logical_simulation", {}).get("operators", [])
        if logical_operator_list: #如果有逻辑仿真
            max_round = task_params.get("logical_simulation", {}).get("round", 0)
            current_round = self._task_repo.get_item_value(task_id=task_id, item="logical_round")
            if current_round >= max_round: #大于防止误输入
                logical_operator = self._task_repo.get_item_value(task_id=task_id, item="logical_operator")
                last_operator = logical_operator_list[-1]
                if logical_operator == last_operator.get("name", ""):
                    try:
                        logical_success_num = self._task_repo.get_item_value(task_id=task_id, item="logical_success_num")
                        number_phones = last_operator.get("simulation_params", {}).get("number_phones", 0)
                        dynamic_phones = last_operator.get("simulation_params", {}).get("dynamic_phones", 0)
                        if logical_success_num >= number_phones - dynamic_phones: #成功的端侧数大于设定端数-动态端数
                            logical_finished_flag = True
                        else:
                            logical_finished_flag = False
                    except Exception as e:
                        logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                     message=f"[calculate_finished_condition]: calculate_finished_condition of task_id={task_id} in logical simulation failed, because of {e}")
                        logical_finished_flag = False
                else:
                    logical_finished_flag = False
            else:
                logical_finished_flag = False
        else:
            logical_finished_flag = True
        # device simulation
        if task_params.get("device_simulation", {}).get("device_requirement", []): #如果有真机侧处理
            try:
                phone_success_num = self._task_repo.get_item_value(task_id=task_id, item="phone_success_num")
                simulate_num = task_params.get("device_simulation", {}).get("simulate_num", 0)
                dynamic_num = task_params.get("device_simulation", {}).get("dynamic_num", 0)
                if phone_success_num >= simulate_num - dynamic_num:  # 成功的端侧数大于设定端数-动态端数
                    device_finished_flag = True
                else:
                    device_finished_flag = False
            except Exception as e:
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                             message=f"[calculate_finished_condition]: calculate_finished_condition of task_id={task_id} in device simulation failed, because of {e}")
                device_finished_flag = False
        else:
            device_finished_flag = True
        return logical_finished_flag, device_finished_flag

    def getTaskQueue(self, request, context):
        taskIDsQueue = self._task_queue.getTaskIDsQueue()
        return taskService__pb2.TaskQueue(tasks=taskIDsQueue)

    def deleteTaskinTaskQueue(self, task_id):
        # 反向遍历
        queueIdx = len(self._task_queue.getTaskQueue()) - 1
        while queueIdx >= 0:
            task_queue = self._task_queue.getTaskQueue()
            if task_queue[queueIdx].taskID.taskID == task_id:
                del task_queue[queueIdx]
            queueIdx = queueIdx - 1

    def checkTaskInTaskQueue(self, taskID):
        # 反向遍历
        queueIdx = len(self._task_queue.getTaskQueue()) - 1
        while queueIdx>=0:
            task_queue = self._task_queue.getTaskQueue()
            if task_queue[queueIdx].taskID.taskID == taskID:
                return True
            queueIdx = queueIdx - 1
        return False

    def changeScheduler(self, request, context):
        pass

    def releaseResource(self):
        while self._running_flag:
            if len(self._task_running) > 0: # 如果有占用资源的任务
                res_running = list()
                for i in range(len(self._task_running)):
                    task_res = self._task_running[i]
                    task_id, task = task_res.get("task_id", ""), task_res.get("task", "")
                    taskStatusMsg = self.get_task_status(task_id=task_id)
                    if taskStatusMsg in [TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.STOPPED]:
                        logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                    message=f"[releaseResource]: The task_status of {task_id} is {taskStatusMsg}!")
                        # stop_task
                        if self.stop_task(task_id=task_id):
                            # release_task
                            resource_occupied = self._task_repo.get_item_value(task_id=task_id, item="resource_occupied")
                            if resource_occupied != 0:
                                release_scheduler = TaskScheduler(resmgr=self._resmgr, phonemgr=self._phonemgr)
                                release_status = release_scheduler.release(task=task)
                                if release_status:
                                    if self._task_repo.set_item_value(task_id=task_id, item="resource_occupied", value=0):
                                        logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                                    message=f"[releaseResource]: Succeeded in releasing the resources of {task_id}")
                                    else:
                                        logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                                    message=f"[releaseResource]: Failed to set resource_occupied=0 of {task_id}")
                                else:
                                    res_running.append(self._task_running[i])
                                    logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                                message=f"[releaseResource]: Failed to release the resource of {task_id}!")
                        else:
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[releaseResource]: Failed to stop the task of {task_id}!")
                    else:
                        res_running.append(self._task_running[i])
                self._task_running = res_running
            time.sleep(self._release_sleep_time)

    def run(self):
        self._running_flag = True
        # run
        while self._running_flag:
            task_queue = self._task_queue.getTaskQueue()
            if task_queue:
                task_scheduler = TaskScheduler(
                    resmgr = self._resmgr,
                    phonemgr = self._phonemgr
                )
                task_scheduler_res = task_scheduler.run(task_queue=task_queue)
                if task_scheduler_res is None:
                    logger.info(task_id="", system_name="TaskMgr", module_name="task_manager",
                                message=f"[run]: Failed to run task_scheduler to obtain an available task!")
                else:
                    freezeStatus = task_scheduler.freeze(
                        task=task_scheduler_res.task,
                        task_request=task_scheduler_res.task_request
                    )
                    if freezeStatus:
                        task = task_scheduler_res.task
                        task_id = task.taskID.taskID
                        logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                            message=f"[run]: Succeeded to freeze resource of {task_id}")
                        task_runner = TaskRunner(task)
                        write_to_db(task_repo=self._task_repo, task_id=task_id, item="resource_occupied", value=1,
                                    logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                        # 再次检查task是否在队列中，如果在，则说明没有被停止，可以继续提交，否则提交失败
                        if self.checkTaskInTaskQueue(taskID=task.taskID.taskID):
                            submitStatus = task_runner.submit(raycluster=self._raydashboard, phonemgr=self._phonemgr)
                        else:
                            submitStatus = False
                        self.deleteTaskinTaskQueue(task_id=task.taskID.taskID)  # 从队列里删除
                        if submitStatus: #提交成功
                            task_id = task.taskID.taskID
                            job_id = task_runner._job_id
                            self._task_running.append({
                                "task_id": task_id,
                                "job_id": job_id,
                                "task": task
                            })
                            write_to_db(task_repo=self._task_repo, task_id=task_id, item="job_id", value=job_id,
                                        logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                            write_to_db(task_repo=self._task_repo, task_id=task_id, item="task_status", value=TaskStatus.RUNNING,
                                        logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                            logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                        message=f"[run]: Succeeded to submit task = {task_id}!")
                        else: #提交失败，停止任务，修改状态，释放资源
                            task_id = task.taskID.taskID
                            if task_runner._job_id is not None:
                                write_to_db(task_repo=self._task_repo, task_id=task_id, item="job_id", value=task_runner._job_id,
                                            logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                            if not self.stop_task(task_id):
                                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                             message=f"[run]: Failed to stop_task of task_id={task_id} when submitStatus=False.")
                            write_to_db(task_repo=self._task_repo, task_id=task_id, item="task_status", value=TaskStatus.FAILED,
                                        logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                            # release resource of task
                            if task_scheduler.release(task=task):
                                write_to_db(task_repo=self._task_repo, task_id=task_id, item="resource_occupied", value=0,
                                            logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                                logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                            message=f"[run]: Succeeded to release resource of {task_id} while submit task failed")
                            else:
                                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                            message=f"[run]: Failed to release the resource of {task_id} while submit task failed!")
                    else:
                        task_id = task_scheduler_res.task.taskID.taskID
                        logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                     message=f"[run]: Failed to freeze the resources of {task_id}!")
                        self.deleteTaskinTaskQueue(task_id)
                        # release resource of task
                        if task_scheduler.release(task=task_scheduler_res.task):
                            write_to_db(task_repo=self._task_repo, task_id=task_id, item="resource_occupied", value=0,
                                        logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                            logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                        message=f"[run]: Succeeded to release resource of {task_id} while freeze resource failed")
                        else:
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[run]: Failed to release the resource of {task_id} while freeze resource failed!")
                        write_to_db(task_repo=self._task_repo, task_id=task_id, item="task_status", value=TaskStatus.FAILED,
                                    logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
            else:
                # print("task_queue is empty")
                time.sleep(1)
            time.sleep(self._scheduler_sleep_time)