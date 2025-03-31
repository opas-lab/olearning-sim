import os
import time
import json
import grpc
import copy
import threading
import configparser
import concurrent.futures
from queue import Queue
from ray.job_submission import JobSubmissionClient
from ols.proto import taskService_pb2 as taskService__pb2
from ols.proto.taskService_pb2_grpc import TaskMgrServicer
from ols.proto import phoneMgr_pb2 as phoneMgr__pb2
from ols.proto import phoneMgr_pb2_grpc  # 导入真机侧的proto
from ols.taskMgr.task_queue import TaskQueue
from ols.taskMgr.task_scheduler import TaskScheduler
from ols.taskMgr.task_runner import TaskRunner
from ols.simu_log import Logger
from ols.taskMgr.utils.utils import TaskTableRepo, write_to_db
from ols.taskMgr.utils.utils import ValidateParameters, json2taskconfig, taskconfig2json
from ols.taskMgr.utils.utils import get_current_time, get_time_difference
from ols.taskMgr.utils.utils import check_task_use_deviceflow, check_deviceflow_dispatch_finished, unregister_task_in_deviceflow

from ols.taskMgr.utils.utils_redis import RedisRepo

# filepath
FILEPATH = os.path.abspath(os.path.dirname(__file__))   # task_manager.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '..')) # ols所在文件夹的绝对路径
CONFIG_PATH = OLSPATH + "/config/config.conf"
# task_table_path
TASK_TABLE_PATH = OLSPATH + "/config/taskmgr_table.yaml"
# redis_path
REDIS_PATH = OLSPATH + "/config/redis.yaml"
# log
logger = Logger()
logger.set_logger(log_yaml=OLSPATH + "/config/repo_log.yaml")

import datetime

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

        # # # redis
        # try:
        #     self._redis_repo = RedisRepo(yaml_path=REDIS_PATH)
        # except Exception as e:
        #     # print(e)
        #     logger.error(task_id="", system_name="TaskMgr", module_name="task_manager",
        #                  message=f"[__init__]: Initiate self._redis_repo failed, because of {e}")
        #     raise e

        self._task_queue = TaskQueue()
        self._task_running = Queue() # 占用资源的任务，使用task_table后，则不存入内存
        self.get_config_params()
        self.get_taskqueue_from_repo()
        self.futures = Queue()
        ### run thread ###
        self._task_queue_thread = threading.Thread(target=self.run) #任务运行线程
        self._task_queue_thread.start()
        self._releaseResource_thread = threading.Thread(target=self.releaseResource) #任务资源释放线程
        self._releaseResource_thread.start()
        self._interruptTask_thread = threading.Thread(target=self.interruptTask) #任务定时中断线程
        self._interruptTask_thread.start()
        # self._task_submit_thread = threading.Thread(target=self.submit_task_to_queue)  # 放入任务队列
        # self._task_submit_thread.start()


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
                    scheduler = TaskScheduler(resmgr=self._resmgr)
                    if scheduler.release(task=taskconfig):
                        write_to_db(task_repo=self._task_repo, task_id=task_id,
                                    item="resource_occupied", value=0,
                                    logger=logger, system_name="TaskMgr",
                                    module_name="task_manager", func_name="get_taskqueue_from_repo")
                        logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                    message=f"[get_taskqueue_from_repo]: Release the resources of"
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
                self._task_running.put({
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
                self._deviceflow_config = OLSPATH + "/config/deviceflow_config.yaml"
                self._scheduler_sleep_time = int(self._config.get("taskMgr", "scheduler_sleep_time"))
                self._release_sleep_time = int(self._config.get("taskMgr", "release_sleep_time"))
                self._interrupt_sleep_time = int(self._config.get("taskMgr", "interrupt_sleep_time"))
                self._interrupt_queue_time = int(self._config.get("taskMgr", "interrupt_queue_time"))
                self._interrupt_running_time = int(self._config.get("taskMgr", "interrupt_running_time"))

            except Exception as e:
                logger.error(task_id="", system_name="TaskMgr", module_name="task_manager",
                             message=f"[get_config_params]: Initiate from config.conf failed,"
                                     f" because of {e}")
                raise e
        else:
            logger.error(task_id="", system_name="TaskMgr", module_name="task_manager",
                        message=f"[get_config_params]: Can not find config file from {CONFIG_PATH}")
            raise Exception(f"Can not find config file from {CONFIG_PATH}")

    ############ 直接提交 #############
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
       # # 参数校验
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
       task_json = taskconfig2json(request)
       if not self._task_repo.set_item_value(task_id=task_id, item="task_params", value=json.dumps(task_json)):
           return taskService__pb2.OperationStatus(is_success=False)
       # 向数据库里记录total_simulation
       data_list = task_json.get("target", {}).get("data", [])
       target_data_list = []
       data_name_list = []
       for data in data_list:
           target_data_dict = {
               "name": data.get("name", ""),
               "simulation_target": data.get("total_simulation", {})
           }
           target_data_list.append(target_data_dict)
           data_name_list.append(data.get("name", ""))
       operatorflow = task_json.get("operatorflow", {})
       operators = operatorflow.get("operators", [])
       operator_name_list = [operator.get("name", "") for operator in operators]
       total_simulation = {
           "max_round": operatorflow.get("flow_setting", {}).get("round", 0),
           "operator_name_list": operator_name_list,
           "data_name_list": data_name_list,
           "total_simulation": target_data_list
       }
       if not self._task_repo.set_item_value(task_id=task_id, item="total_simulation", value=json.dumps(total_simulation)):
           return taskService__pb2.OperationStatus(is_success=False)
       # 向任务队列添加任务
       self._task_queue.add(request)
       # 向数据库表中添加任务进入队列的时间
       if not self._task_repo.set_item_value(task_id=task_id, item="in_queue_time", value=get_current_time(mode='Asia/Shanghai')):
           logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                       message=f"[submitTask]: Set in_queue_time of task_id={task_id} failed.")
       logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                   message=f"[submitTask]: Add task = {task_id} to TaskQueue")
       return taskService__pb2.OperationStatus(is_success=True)

    # ############# 使用Redis #############
    # def submitTask(self, request, context):
    #     # print(request)
    #     # 参数校验
    #     validation = ValidateParameters()
    #     if not validation.validate_task_parameters(request):
    #         # 将任务状态设置为FAILED
    #         try:
    #             self._task_repo.set_item_value(task_id=request.taskID.taskID, item="task_status", value=TaskStatus.FAILED)
    #         except:
    #             pass
    #         return taskService__pb2.OperationStatus(is_success=False)
    #
    #     # task_id = request.taskID.taskID
    #     # request.taskID.taskID = f"{task_id}_{str(uuid.uuid4())}"
    #
    #     # 任务参数
    #     task_json = taskconfig2json(request)
    #     task_json_string = json.dumps(task_json)
    #
    #     is_success = self._redis_repo.insert_data(value=task_json_string)
    #     if not is_success:
    #         return taskService__pb2.OperationStatus(is_success=False)
    #     return taskService__pb2.OperationStatus(is_success=True)
    #
    # def submit_task_to_queue(self):
    #     self._running_flag = True
    #     while self._running_flag:
    #         task_json_string = self._redis_repo.pop_data()
    #         if task_json_string is not None:
    #             # print(f"[submit_task_to_queue]: task_json_string = {task_json_string}")
    #             try:
    #                 task_json = json.loads(task_json_string)
    #             except:
    #                 continue
    #             request = json2taskconfig(task_json_string)
    #             task_id = request.taskID.taskID
    #
    #             # 检验task_id是否在数据库
    #             if not self._task_repo.check_task_in_database(task_id=task_id):
    #                 logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
    #                              message=f"[submitTask]: task_id={task_id} is not in the task_table")
    #                 # TODO: 这块可能需要和后端一起看看，目前的逻辑是后端会写入到数据库表里，按此逻辑，如果task_id不在数据库里，则是错误的，无法添加到队列中。状态返回是MISSING。
    #                 continue
    #
    #             # 检验task_id的对应task_status是否为UNDONE
    #             task_status = self._task_repo.get_item_value(task_id=task_id, item="task_status")
    #             if task_status != TaskStatus.UNDONE:
    #                 logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
    #                              message=f"[submitTask]: task_id={task_id} once existed, and the status is {task_status}")
    #                 continue
    #
    #             # 向数据库里记录total_simulation
    #             data_list = task_json.get("target", {}).get("data", [])
    #             target_data_list = []
    #             data_name_list = []
    #             for data in data_list:
    #                 target_data_dict = {
    #                     "name": data.get("name", ""),
    #                     "simulation_target": data.get("total_simulation", {})
    #                 }
    #                 target_data_list.append(target_data_dict)
    #                 data_name_list.append(data.get("name", ""))
    #             operatorflow = task_json.get("operatorflow", {})
    #             operators = operatorflow.get("operators", [])
    #             operator_name_list = [operator.get("name", "") for operator in operators]
    #             total_simulation = {
    #                 "max_round": operatorflow.get("flow_setting", {}).get("round", 0),
    #                 "operator_name_list": operator_name_list,
    #                 "data_name_list": data_name_list,
    #                 "total_simulation": target_data_list
    #             }
    #             total_simulation_string = json.dumps(total_simulation)
    #             # 入队时间
    #             in_queue_time = get_current_time(mode='Asia/Shanghai')
    #
    #             # 增加数据条目
    #             is_set_item_values = self._task_repo.set_item_values(
    #                 identify_name = "task_id",
    #                 identify_value = task_id,
    #                 values_to_update = {
    #                     "task_status": ["QUEUED"],
    #                     "task_params": [task_json_string],
    #                     "total_simulation": [total_simulation_string],
    #                     "in_queue_time": in_queue_time
    #                 }
    #             )
    #             if is_set_item_values:
    #                 self._task_queue.add(request)
    #         else:
    #             time.sleep(self._scheduler_sleep_time)


    def stopTask(self, request, context):
        try:
            task_id = request.taskID
        except Exception as e:
            logger.error(task_id="", system_name="TaskMgr", module_name="task_manager",
                        message=f"[stopTask]: Extract task_id from request = {request} failed, because of {e}")
            return taskService__pb2.OperationStatus(is_success=False)
        stop_status = self.stop_task(task_id=task_id)
        return taskService__pb2.OperationStatus(is_success=stop_status)

    def stop_task(self, task_id, job_id=None):
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
                taskconfig = taskconfig,
                job_id = job_id
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

    def stop_operation(self, task_id, task_status, taskconfig, job_id=None):
        # 逻辑仿真
        if taskconfig.logicalSimulation.resourceRequestLogicalSimulation:
            # 逻辑仿真部分的停止
            db_job_id = self._task_repo.get_item_value(task_id=task_id, item="job_id")
            # 如果被清内存的，就用数据库的job_id替换一下
            if job_id == None:
                job_id = db_job_id

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
        if taskconfig.deviceSimulation.resourceRequestDeviceSimulation: # 使用了真机侧
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
        try:
            task_id = request.taskID
        except Exception as e:
            logger.error(task_id="", system_name="TaskMgr", module_name="task_manager",
                        message=f"[getTaskStatus]: Extract task_id from request = {request} failed, because of {e}")
            taskService__pb2.TaskStatus(taskStatus=taskService__pb2.TaskStatusEnum.MISSING)
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
                        write_to_db(task_repo=self._task_repo, task_id=task_id, item="task_status",
                                    value=TaskStatus.FAILED, logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                        return TaskStatus.FAILED
                    else:
                        try:
                            taskconfig = json2taskconfig(task_params_string)
                        except:
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[get_task_status]: task_params of task_id={task_id} does not meet submission requirements, failed!")
                            write_to_db(task_repo=self._task_repo, task_id=task_id, item="task_status",
                                        value=TaskStatus.FAILED, logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                            return TaskStatus.FAILED
                    # 仿真任务状态查询
                    if taskconfig.logicalSimulation.resourceRequestLogicalSimulation:
                        try:
                            job_id = self._task_repo.get_item_value(task_id=task_id, item="job_id")
                            client = JobSubmissionClient(self._raydashboard)
                            task_status = client.get_job_status(job_id=job_id)
                            logical_task_status = str(task_status)
                        except Exception as e:
                            # 查询是否真的有申请逻辑仿真资源任务
                            logical_target = self._task_repo.get_item_value(task_id=task_id, item="logical_target")
                            if logical_target is None:
                                logical_task_status = TaskStatus.SUCCEEDED
                            else:
                                write_to_db(task_repo=self._task_repo, task_id=task_id, item="task_status",
                                            value=TaskStatus.FAILED, logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                                return TaskStatus.FAILED
                    else:
                        logical_task_status = TaskStatus.SUCCEEDED
                    # 真机任务状态查询
                    if taskconfig.deviceSimulation.resourceRequestDeviceSimulation:
                        try:
                            with grpc.insecure_channel(self._phonemgr) as channel:
                                stub = phoneMgr_pb2_grpc.TaskManagerStub(channel)
                                taskID = taskService__pb2.TaskID(taskID=task_id)
                                device_task_result_config = stub.getDeviceTaskStatus(taskID)
                            # print(f"[task_manager]: device_task_result_config = {device_task_result_config}")
                            # 构造device_task_result的dict形式
                            device_data_list_config = list(device_task_result_config.deviceDataStatus)
                            # print(f"[task_manager]: device_data_list_config = {device_data_list_config}")
                            device_data_list = []
                            for device_data_config in device_data_list_config:
                                device_data = {}
                                device_data.update({
                                    "name": str(device_data_config.name),
                                    "simulation_target": {
                                        "devices": list(device_data_config.deviceType),
                                        "success_num": list(device_data_config.successNum),
                                        "failed_num": list(device_data_config.failedNum)
                                    }
                                })
                                device_data_list.append(device_data)
                            device_task_result = {
                                "is_finished": device_task_result_config.isFinished,
                                "device_result": device_data_list,
                            }

                            # 将查询结果写入数据库
                            write_to_db(task_repo=self._task_repo, task_id=task_id,
                                        item="device_round", value=int(device_task_result_config.round),
                                        logger=logger, system_name="TaskMgr",
                                        module_name="task_manager", func_name="get_task_status")
                            write_to_db(task_repo=self._task_repo, task_id=task_id,
                                        item="device_operator", value=str(device_task_result_config.operator),
                                        logger=logger, system_name="TaskMgr",
                                        module_name="task_manager", func_name="get_task_status")
                            write_to_db(task_repo=self._task_repo, task_id=task_id,
                                        item="device_result", value=json.dumps(device_task_result),
                                        logger=logger, system_name="TaskMgr",
                                        module_name="task_manager", func_name="get_task_status")
                        except Exception as e:
                            import traceback
                            print(f"Get phone status error, because of {e}, traceback = \n{traceback.format_exc()}")
                            return TaskStatus.FAILED
                    else:
                        device_task_result = {}

                    # 综合logical_task_status、device_task_status以及各任务允许的动态手机数
                    final_task_status = self.combine_task_status(
                        task_id=task_id, logical_task_status=logical_task_status, device_task_result=device_task_result
                    )
                    # print(f"final_task_status = {final_task_status}")
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

    def combine_task_status(self, task_id, logical_task_status, device_task_result):
        device_task_status = device_task_result.get("is_finished", True)

        task_params_string = self._task_repo.get_item_value(task_id=task_id, item="total_simulation")
        if task_params_string is None:
            logger.error(task_id=task_id,
                         system_name="TaskMgr",
                         module_name="task_manager",
                         message=f"[get_task_status]: Get total_simulation of task_id={task_id} is None, failed!")
            return TaskStatus.FAILED
        else:
            task_params = json.loads(task_params_string)

        logical_success, logical_round_failed, device_success, device_round_failed = self.calculate_conditions(
            task_id=task_id, task_params=task_params, device_task_result=device_task_result
        )
        '''
        logical_success, 逻辑仿真任务是否满足成功条件，True为满足最终成功判断条件，False为不满足最终成功判断条件
        logical_round_failed, 逻辑仿真任务是否因为轮次有失败而提前结束，True为逻辑仿真任务轮次有失败而提前结束，False为逻辑仿真任务正常运行
        logical_task_status: [SUCCEEDED, PENDING, RUNNING, STOPPED, FAILED], 5种状态
        device_success，真机任务是否满足成功条件，True为满足最终成功判断条件，False为不满足最终成功判断条件
        device_round_failed, 真机任务因为轮次有失败而提前结束，True为真机任务轮次有失败而提前结束，False为真机任务正常运行
        device_task_status: True为真机任务已完成，False为真机任务未完成

        总共为2*2*2*2*5*2=160种状态
        考虑: 
        (1) logical_success=True, logical_round_failed=True
        (2) device_success=True, device_round_failed=True
        不存在以上2种状态，因此总状态为160-70=90种

        状态判断，任务状态有4种，SUCCEEDED, STOPPED, RUNNING, FAILED
        SUCCEEDED: 有效状态为10种
            logical_success==True, device_success==True
        STOPPED: 有效状态为2种
            logical_success == False, 
            logical_round_failed == False,
            logical_task_status == TaskStatus.STOPPED,
            device_round_failed == False,
            device_task_status == True
        FAILED: 共67种
            (1) 逻辑仿真正常结束, 有效状态为34种
            logical_success == False
            logical_task_status in [TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.STOPPED]
            (2) 逻辑仿真提前结束，有效状态为12种
            logical_success == False
            logical_round_failed == True
            (3) 真机任务正常结束，有效状态14种
            device_success == False
            device_task_status == True
            (4) 真机任务提前结束，有效状态7种
            device_success == False
            device_round_failed == True
        RUNNING: 共11种
        '''
        # print(f"logical_success = {logical_success}")
        # print(f"logical_round_failed = {logical_round_failed}")
        # print(f"logical_task_status = {logical_task_status}")
        # print(f"device_success = {device_success}")
        # print(f"device_round_failed = {device_round_failed}")
        # print(f"device_task_status = {device_task_status}")
        # 判断不存在的状态
        if logical_success and logical_round_failed:
            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                         message=f"[combine_task_status]: logical_success and logical_round_failed are both True, invalid logical task status!")
            return TaskStatus.FAILED
        if device_success and device_round_failed:
            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                         message=f"[combine_task_status]: device_success and device_round_failed are both True, invalid device task status!")
            return TaskStatus.FAILED
        # SUCCEEDED
        if logical_success and device_success:
            return TaskStatus.SUCCEEDED
        # STOPPED
        if logical_success == False and logical_round_failed == False and logical_task_status == TaskStatus.STOPPED \
                and device_round_failed == False and device_task_status == True:
            return TaskStatus.STOPPED
        # FAILED
        if logical_success == False and logical_task_status in [TaskStatus.SUCCEEDED, TaskStatus.FAILED,
                                                                TaskStatus.STOPPED]:
            return TaskStatus.FAILED
        if logical_success == False and logical_round_failed == True:
            return TaskStatus.FAILED
        if device_success == False and device_task_status == True:
            return TaskStatus.FAILED
        if device_success == False and device_round_failed == True:
            return TaskStatus.FAILED
        # RUNNING
        return TaskStatus.RUNNING

    def calculate_conditions(self, task_id, task_params, device_task_result):
        # get total_simulation params
        max_round = task_params.get("max_round", 0)
        operator_name_list = task_params.get("operator_name_list", [])
        data_name_list = task_params.get("data_name_list", [])
        total_simulation = task_params.get("total_simulation", [])
        last_operator_name = operator_name_list[-1]

        # logical simulation
        '''
        需要根据轮次判断, 获得logical_success、logical_round_failed
        logical_success, 逻辑仿真任务是否满足成功条件，
            True: 最后一轮，算子为最后一个，成功端侧数 大于等于 设定端数-动态端数
            False: 其他情况
        logical_round_failed, 逻辑仿真任务是否因为轮次有失败而提前结束
            True: 根据算子参数设置，成功端侧数 小于 设定端数-动态端数
            False: 其他情况
        理论上存在4种情况：
        （1）logical_success=True, logical_round_failed=True   -> 不存在，有矛盾
        （2）logical_success=True, logical_round_failed=False  -> 存在，逻辑仿真正常成功
        （3）logical_success=False, logical_round_failed=True  -> 存在，提前结束，逻辑仿真失败
        （4）logical_success=False, logical_round_failed=False -> 存在，逻辑仿真失败，或逻辑仿真正在运行
        '''
        # get logical_target
        logical_target_string = self._task_repo.get_item_value(task_id=task_id, item="logical_target")
        logical_result, logical_data_name_list = [], []
        logical_current_round, logical_operator_name = None, None
        if logical_target_string != None: # 如果有逻辑仿真
            logical_success, logical_round_failed = False, False
            logical_result_string = self._task_repo.get_item_value(task_id=task_id, item="logical_result")
            if logical_result_string != None: # 有返回结果才可以判断任务状态，否则仍处于正在运行状态
                logical_target_dict = json.loads(logical_target_string)
                logical_target = logical_target_dict.get("logical_target", [])
                logical_result_dict = json.loads(logical_result_string)
                logical_result = logical_result_dict.get("logical_result", [])
                logical_current_round = self._task_repo.get_item_value(task_id=task_id, item="logical_round")
                logical_operator_name = self._task_repo.get_item_value(task_id=task_id, item="logical_operator")
                logical_data_name_list = [logical_data_result.get("name", "") for logical_data_result in logical_result]
                # 增加对逻辑仿真完成的判断
                success_comparison_results = []
                if logical_current_round >= max_round and logical_operator_name == last_operator_name:
                    for data_index, data_total in enumerate(total_simulation):
                        data_name = data_name_list[data_index]
                        logical_nums = logical_target[data_index].get("simulation_target", {}).get("nums", [])
                        dynamic_nums = data_total.get("simulation_target", {}).get("dynamic_nums", [])
                        if data_name in logical_data_name_list:
                            logical_data_index = logical_data_name_list.index(data_name)
                            logical_result_success_nums = logical_result[logical_data_index].get("simulation_target",{}).get("success_num",[])
                            # print(f"logical_result_success_nums = {logical_result_success_nums}, nums={logical_nums}, dynamic_nums={dynamic_nums}")
                            success_comparison_result = all([x >= a - z for x, a, z in zip(logical_result_success_nums, logical_nums, dynamic_nums)])
                            # print(f"success_comparison_result = {success_comparison_result}")
                            success_comparison_results.append(success_comparison_result)
                    # print(f"success_comparison_results = {success_comparison_results}")
                    if len(success_comparison_results)>0:
                        if all(success_comparison_results):
                            logical_success, logical_round_failed = True, False
        else:
            logical_success, logical_round_failed = True, False

        # device simulation
        '''
        需要根据轮次判断, device_success、device_round_failed
        device_success, 真机任务是否满足成功条件，
            True: 最大轮数的 成功端侧数 大于等于 设定端数-动态端数
            False: 其他情况
        device_round_failed, 真机任务是否因为轮次有失败而提前结束
            True: 如果失败端数 > 动态端数
            False: 其他情况
        理论上存在4种情况：
        （1）device_success=True, device_round_failed=True   -> 不存在，有矛盾
        （2）device_success=True, device_round_failed=False  -> 存在，真机任务正常成功
        （3）device_success=False, device_round_failed=True  -> 存在，提前结束，真机任务失败
        （4）device_success=False, device_round_failed=False -> 存在，真机任务失败，或真机任务正在运行
        '''
        device_target_string = self._task_repo.get_item_value(task_id=task_id, item="device_target")
        device_result, device_data_name_list = [], []
        device_current_round, device_operator_name = None, None
        if device_target_string != None:
            device_success, device_round_failed = False, False
            if device_task_result.get("device_result", []) != []:
                device_target_dict = json.loads(device_target_string)
                device_target = device_target_dict.get("device_target", [])
                device_result = device_task_result.get("device_result", [])
                device_current_round = self._task_repo.get_item_value(task_id=task_id, item="device_round")
                device_operator_name = self._task_repo.get_item_value(task_id=task_id, item="device_operator")
                device_data_name_list = [device_data_result.get("name", "") for device_data_result in device_result]
                # 增加对真机仿真完成的判断
                success_comparison_results = []
                if device_current_round >= max_round and device_operator_name == last_operator_name:
                    for data_index, data_total in enumerate(total_simulation):
                        data_name = data_name_list[data_index]
                        device_nums = device_target[data_index].get("simulation_target", {}).get("nums", [])
                        dynamic_nums = data_total.get("simulation_target", {}).get("dynamic_nums", [])
                        if data_name in device_data_name_list:
                            device_data_index = device_data_name_list.index(data_name)
                            device_result_success_nums = device_result[device_data_index].get("simulation_target", {}).get("success_num", [])
                            success_comparison_result = all([x >= a - z for x, a, z in zip(device_result_success_nums, device_nums, dynamic_nums)])
                            # print(f"device success_comparison_result = {success_comparison_result}")
                            success_comparison_results.append(success_comparison_result)
                    # print(f"device success_comparison_results = {success_comparison_results}")
                    if len(success_comparison_results) > 0:
                        if all(success_comparison_results):
                            device_success, device_round_failed = True, False
        else:
            device_success, device_round_failed = True, False

        # 综合逻辑仿真和真机
        combine_data_status = []
        for data_index, data_total in enumerate(total_simulation):
            data_name = data_name_list[data_index]
            nums = data_total.get("simulation_target", {}).get("nums", [])
            dynamic_nums = data_total.get("simulation_target", {}).get("dynamic_nums", [])
            if dynamic_nums == []:
                dynamic_nums = [0] * len(nums)

            # 逻辑仿真
            if data_name in logical_data_name_list:
                logical_data_index = logical_data_name_list.index(data_name)
                # 逻辑仿真失败数
                logical_result_failed_nums = logical_result[logical_data_index].get("simulation_target", {}).get("failed_num", [])
                # 逻辑仿真成功数
                logical_result_success_nums = logical_result[logical_data_index].get("simulation_target", {}).get("success_num", [])
            else:
                logical_result_failed_nums = [0] * len(dynamic_nums)
                logical_result_success_nums = [0] * len(nums)

            # 真机仿真
            if data_name in device_data_name_list:
                device_data_index = device_data_name_list.index(data_name)
                # 真机仿真失败数
                device_result_failed_nums = device_result[device_data_index].get("simulation_target", {}).get("failed_num", [])
                # 逻辑仿真成功数
                device_result_success_nums = device_result[device_data_index].get("simulation_target", {}).get("success_num", [])
            else:
                device_result_failed_nums = [0] * len(dynamic_nums)
                device_result_success_nums = [0] * len(nums)

            # 判断是否提前失败结束
            failed_comparison_results = []
            # 需要使用单个服务的场景
            if logical_result == [] or device_result == []:
                failed_comparison_results = [x < y + z for x, y, z in
                                             zip(dynamic_nums, logical_result_failed_nums, device_result_failed_nums)]
            # 只在轮次相同的情况下考虑
            if (logical_current_round != None) and (device_current_round != None) and (logical_current_round == device_current_round) and logical_operator_name == device_operator_name:
                failed_comparison_results = [x < y + z for x, y, z in
                                             zip(dynamic_nums, logical_result_failed_nums, device_result_failed_nums)]

            if failed_comparison_results:
                if True in failed_comparison_results:
                    if logical_result == [] and device_result != []:
                        logical_round_failed = False
                        device_round_failed = True
                    elif logical_result != [] and device_result == []:
                        logical_round_failed = True
                        device_round_failed = False
                    else:
                        logical_round_failed = True
                        device_round_failed = True
                    break

            # 判断这个数据是否已经成功完成了
            success_comparison_results = []
            # 需要使用单个服务的场景
            if logical_result == [] or device_result == []:
                success_comparison_results = [x + y >= a - z for x, y, a, z in
                                             zip(logical_result_success_nums, device_result_success_nums, nums, dynamic_nums)]
            if (logical_current_round != None) and (device_current_round != None) and (logical_current_round == device_current_round):
                success_comparison_results = [x + y >= a - z for x, y, a, z in
                                             zip(logical_result_success_nums, device_result_success_nums, nums, dynamic_nums)]
            if success_comparison_results:
                if False in success_comparison_results:
                    combine_data_status.append(False)
                else:
                    combine_data_status.append(True)

        # 提前成功的判据
        # 如果只有逻辑仿真
        if logical_result:
            if logical_current_round >= max_round and logical_operator_name == last_operator_name:
                if combine_data_status and False not in combine_data_status:
                    # print(f"如果只有逻辑仿真, combine_data_status = {combine_data_status}")
                    logical_success = True
        # 如果只有真机仿真
        if device_result:
            if device_current_round >= max_round and device_operator_name == last_operator_name:
                if combine_data_status and False not in combine_data_status:
                    # print(f"如果只有真机仿真, combine_data_status = {combine_data_status}")
                    device_success = True

        return logical_success, logical_round_failed, device_success, device_round_failed

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

    def threading_submit_task(self, task_scheduler_res):
        task_scheduler = TaskScheduler(resmgr=self._resmgr)
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
                # 查验一次resource_occupied是否被准确修改
                try:
                    resource_occupied = self._task_repo.get_item_value(task_id=task_id, item="resource_occupied")
                    if resource_occupied == 1:
                        submitStatus = task_runner.submit(
                            raycluster = self._raydashboard,
                            phonemgr = self._phonemgr,
                            deviceflow_config = self._deviceflow_config
                        )
                    else:
                        submitStatus = False
                except Exception as e:
                    submitStatus = False
                    logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                message=f"[run]: Submit task={task_id} failed, because of {e}")
                if submitStatus:  # 提交成功
                    task_id = task.taskID.taskID
                    job_id = task_runner._job_id
                    self._task_running.put({
                        "task_id": task_id,
                        "job_id": job_id,
                        "task": task
                    })
                    # 关键参数写入数据库
                    write_to_db(task_repo=self._task_repo, task_id=task_id, item="job_id", value=job_id,
                                logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                    write_to_db(task_repo=self._task_repo, task_id=task_id, item="task_status",
                                value=TaskStatus.RUNNING,
                                logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                    write_to_db(task_repo=self._task_repo, task_id=task_id, item="submit_task_time",
                                value=get_current_time(mode='Asia/Shanghai'),
                                logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                    db_job_id = self._task_repo.get_item_value(task_id=task_id, item="job_id")
                    task_submitted_time = self._task_repo.get_item_value(task_id=task_id, item="submit_task_time")

                    # 如果没有正常写入job_id
                    if job_id != db_job_id:
                        try:
                            client = JobSubmissionClient(self._raydashboard)
                            client.stop_job(job_id=job_id)
                            logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                        message=f"[run]: stopping task_id={task_id} with job_id={job_id} from Ray, "
                                                f"when job_id can not be written in database!")
                            write_to_db(task_repo=self._task_repo, task_id=task_id, item="task_status",
                                        value=TaskStatus.FAILED, logger=logger, system_name="TaskMgr",
                                        module_name="task_manager", func_name="run")
                        except Exception as e:
                            write_to_db(task_repo=self._task_repo, task_id=task_id, item="task_status",
                                        value=TaskStatus.FAILED, logger=logger, system_name="TaskMgr",
                                        module_name="task_manager", func_name="run")
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[run]: stopping task_id={task_id} with job_id={job_id} from Ray failed, because of {e}")

                    # 如果无法正常提交task_submmitted_time，会导致无法中断任务，所以直接stop报Failed会更好
                    if task_submitted_time == None:
                        stop_submmited_task = self.stop_task(task_id=task_id)
                        if stop_submmited_task:
                            logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                        message=f"[run]: stop task_id={task_id} when task_submitted_time can not be written in database!")
                        else:
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[run]: failed to stop task_id={task_id} when task_submitted_time can not be written in database!")

                    logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                message=f"[run]: Succeeded to submit task = {task_id}!")
                else:  # 提交失败，停止任务，修改状态，释放资源
                    task_id = task.taskID.taskID
                    if task_runner._job_id is not None:
                        write_to_db(task_repo=self._task_repo, task_id=task_id, item="job_id",
                                    value=task_runner._job_id,
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
                        # 任务提交失败，停止成功，资源释放失败之后, 放到运行队列_task_running里
                        self._task_running.put({
                            "task_id": task_id,
                            "job_id": None,
                            "task": task
                        })
                        logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                     message=f"[run]: Failed to release the resource of {task_id} while submit task failed!")
            else:
                task_id = task_scheduler_res.task.taskID.taskID
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                             message=f"[run]: Failed to freeze the resources of {task_id}!")
                # 冻结失败，只有混合任务才需要释放资源
                if (task_scheduler_res.task.logicalSimulation.resourceRequestLogicalSimulation and
                        task_scheduler_res.task.deviceSimulation.resourceRequestDeviceSimulation):
                    # release resource of task
                    if task_scheduler.release(task=task_scheduler_res.task):
                        write_to_db(task_repo=self._task_repo, task_id=task_id, item="resource_occupied", value=0,
                                    logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                        logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                    message=f"[run]: Succeeded to release resource of {task_id} while freeze resource failed")
                    else:
                        # 释放失败，重新放入队列中进行释放
                        self._task_running.put({
                            "task_id": task_id,
                            "job_id": None,
                            "task": task_scheduler_res.task
                        })
                        logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                     message=f"[run]: Failed to release the resource of {task_id} while freeze resource failed!")

                write_to_db(task_repo=self._task_repo, task_id=task_id, item="task_status", value=TaskStatus.FAILED,
                            logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")

    def run(self):
        self._running_flag = True
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # run
            while self._running_flag:
                task_queue = self._task_queue.getTaskQueue()
                if task_queue:
                    task_scheduler = TaskScheduler(resmgr=self._resmgr)
                    task_scheduler_res = task_scheduler.run(task_queue=task_queue)
                    if task_scheduler_res is not None:
                        task_id = task_scheduler_res.task.taskID.taskID
                        self.deleteTaskinTaskQueue(task_id=task_id) # 调度出来后，从队列里删除
                        executor.submit(self.threading_submit_task, task_scheduler_res)
                else:
                    # print("task_queue is empty")
                    time.sleep(1)
                time.sleep(self._scheduler_sleep_time)

    def releaseResource(self):
        while self._running_flag:
            if not self._task_running.empty(): # 如果有占用资源的任务
                res_running = Queue()
                while not self._task_running.empty():
                    task_res = self._task_running.get()
                    task_id, task = task_res.get("task_id", ""), task_res.get("task", "")
                    job_id = task_res.get("job_id", None)
                    taskStatusMsg = self.get_task_status(task_id=task_id)
                    if taskStatusMsg in [TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.STOPPED]:
                        logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                    message=f"[releaseResource]: The task_status of {task_id} is {taskStatusMsg}!")
                        # stop_task
                        if self.stop_task(task_id=task_id, job_id=job_id):
                            # release_task
                            resource_occupied = self._task_repo.get_item_value(task_id=task_id, item="resource_occupied")
                            if resource_occupied != 0:
                                release_scheduler = TaskScheduler(resmgr=self._resmgr)
                                release_status = release_scheduler.release(task=task)
                                if release_status:
                                    if self._task_repo.set_item_value(task_id=task_id, item="resource_occupied", value=0):
                                        logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                                    message=f"[releaseResource]: Succeeded in releasing the resources of {task_id}")
                                    else:
                                        logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                                    message=f"[releaseResource]: Failed to set resource_occupied=0 of {task_id}")
                                    # 记录结束时间
                                    write_to_db(task_repo=self._task_repo, task_id=task_id, item="task_finished_time", value=get_current_time(mode='Asia/Shanghai'),
                                                logger=logger, system_name="TaskMgr", module_name="task_manager", func_name="run")
                                else:
                                    res_running.put(task_res)
                                    logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                                message=f"[releaseResource]: Failed to release the resource of {task_id}!")
                            # TODO: 如果该任务要使用Deviceflow, 需要UnRegisterTask, 保证变量生命周期完整
                            if check_task_use_deviceflow(task):
                                if taskStatusMsg in [TaskStatus.SUCCEEDED]:
                                    # 需要检查是否已经dispatch成功了（存在仿真结束了，但梯度中间站没发完消息，此时不能unregister）
                                    if check_deviceflow_dispatch_finished(task_id):
                                        is_unregister = unregister_task_in_deviceflow(task_id)
                                        if not is_unregister:
                                            res_running.put(task_res)
                                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                                         message=f"[releaseResource]: Failed to unregister the deviceflow of {task_id}!")
                                    else:
                                        res_running.put(task_res)
                                else:
                                    is_unregister = unregister_task_in_deviceflow(task_id)
                                    if not is_unregister:
                                        res_running.put(task_res)
                                        logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                                     message=f"[releaseResource]: Failed to unregister the deviceflow of {task_id}!")

                        else:
                            res_running.put(task_res) # 停止失败也放入队列中，进行下一次停止
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[releaseResource]: Failed to stop the task of {task_id}!")
                    else:
                        if taskStatusMsg != TaskStatus.MISSING:
                            res_running.put(task_res)
                        # credit to fy and djj
                        # 任务停止失败或资源释放失败，放回队列里再试一次，直到成功，但不需要写DB
                        else:
                            if self.stop_operation(
                                task_id = task_id,
                                task_status = TaskStatus.MISSING,
                                taskconfig = task,
                                job_id = job_id
                            ):
                                release_scheduler = TaskScheduler(resmgr=self._resmgr)
                                if release_scheduler.release(task=task):
                                    logger.info(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                                message=f"[releaseResource]: Succeeded in releasing the resources of {task_id} when task_status is missing!")
                                else:
                                    res_running.put(task_res)
                            else:
                                res_running.put(task_res)
                self._task_running = res_running
            time.sleep(self._release_sleep_time)

    def interruptTask(self):
        while self._running_flag:
            # 任务已被提交，耗时超时，停止任务
            if not self._task_running.empty(): # 如果有占用资源的任务
                tmp_task_running = copy.copy(self._task_running)
                for task_res in list(tmp_task_running.queue):
                    task_id = task_res.get("task_id", "")
                    task_submitted_time = str(self._task_repo.get_item_value(task_id=task_id, item="submit_task_time"))
                    interrupt_running_flag = False
                    try:
                        current_time = get_current_time(mode='Asia/Shanghai')
                        time_difference = get_time_difference(start_time=task_submitted_time, end_time=current_time)
                        if time_difference > self._interrupt_running_time:
                            interrupt_running_flag = True
                    except Exception as e:
                        interrupt_running_flag = True
                        if task_submitted_time is None:
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[interruptTask]: submit_task_time of {task_id} is None, failed!")
                        else:
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[interruptTask]: submit_task_time of {task_id} is invalid, failed!")
                    if interrupt_running_flag == True:
                        if not self.stop_task(task_id=task_id): #直接停止任务
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[interruptTask]: Failed to stop the task of {task_id}!") #停止任务失败
            # 任务已进入队列，长时间未被调度执行
            task_queue = self._task_queue.getTaskQueue()
            if task_queue:
                for i in range(len(task_queue)):
                    task_id = task_queue[i].taskID.taskID
                    task_in_queue_time = str(self._task_repo.get_item_value(task_id=task_id, item="in_queue_time"))
                    interrupt_queue_flag = False
                    try:
                        current_time = get_current_time(mode='Asia/Shanghai')
                        time_difference = get_time_difference(start_time=task_in_queue_time, end_time=current_time)
                        if time_difference > self._interrupt_queue_time:
                            interrupt_queue_flag = True
                    except Exception as e:
                        interrupt_queue_flag = True
                        if task_in_queue_time is None:
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[interruptTask]: in_queue_time of {task_id} is None, failed!")
                        else:
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[interruptTask]: in_queue_time of {task_id} is invalid, failed!")
                    if interrupt_queue_flag == True:
                        if not self.stop_task(task_id=task_id): #直接停止任务
                            logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                                         message=f"[interruptTask]: Failed to stop the task of {task_id}!") #停止任务失败
            time.sleep(self._interrupt_sleep_time)
