import os
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from ols.simu_log import Logger

# filepath
FILEPATH = os.path.abspath(os.path.dirname(__file__))   # task_manager.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '../..')) # ols所在文件夹的绝对路径
# log
logger = Logger()
logger.set_logger(log_yaml = OLSPATH + "/config/repo_log.yaml")

class TaskSchedulerRes: #调度结果
    def __init__(self, task, task_request):
        self.task = task
        self.task_request = task_request

class SchedulerStrategy(ABC):
    @abstractmethod
    def schedule_next_task(self, task_queue: List[Any],
                           available_resources: Dict[str, Any]) -> Any:
        """
        Schedule the next task to be executed.
        Args:
            task_queue (List[Any]): The task queue of queued tasks.
            available_resources (Dict[str, Any]): The total available resources.
        Returns:
            Any: Success: TaskSchedulerRes
                 Failed: None
        Raises:
            NotImplementedError: If the method is not implemented by the concrete subclass.
        """
        raise NotImplementedError("schedule_next_task method must be"
                                  " implemented by the concrete subclass.")

class DefaultStrategy(SchedulerStrategy):
    def get_task_request_resource(self, task):
        task_request = dict()

        # 获取logical_simulation所申请的资源总量
        logical_simulation_config = task.logicalSimulation
        computation_unit_config = logical_simulation_config.computationUnit
        # logical_simulation使用的计算单元总量
        logical_requirement_unit = {}
        for resource_request_config in logical_simulation_config.resourceRequestLogicalSimulation:
            devices_list = list(resource_request_config.deviceResourceRequest)
            nums_list = list(resource_request_config.numResourceRequest)
            for device_index, device_type in enumerate(devices_list):
                num_device_type = int(logical_requirement_unit.get(device_type, 0)) + nums_list[device_index]
                logical_requirement_unit.update(
                    {f"{device_type}": num_device_type}
                )
        # logical_simulation计算单元的设置
        devices_unit = list(computation_unit_config.devicesUnit)
        computation_unit_setting_list = []
        for computation_unit_setting_config in computation_unit_config.unitSetting:
            computation_unit_setting = {
                "num_cpus": computation_unit_setting_config.numCpus
            }
            computation_unit_setting_list.append(computation_unit_setting)
        computation_unit = {}
        for device_index, device_type in enumerate(devices_unit):
            computation_unit.update({
                f"{device_type}": computation_unit_setting_list[device_index]
            })
        # logical_simulation资源的计算
        cpu_request, mem_request = 0.0, 0.0
        for device_type in logical_requirement_unit:
            cpu_request = cpu_request + \
                          computation_unit.get(device_type, {}).get("num_cpus", 0) * \
                          logical_requirement_unit.get(device_type, 0)
            # mem_request = mem_request + \
            #               computation_unit.get(device_type, {}).get("num_mems", 0) * \
            #               logical_requirement_unit.get(device_type, 0)

            ###### 临时为了展示使用 ######
            mem_request = mem_request + \
                          computation_unit.get(device_type, {}).get("num_mems", 1.0) * \
                          logical_requirement_unit.get(device_type, 0)
        task_request.update({
            "logical_simulation": {
                "cpu": cpu_request,
                "mem": mem_request
        }})

        # 获取device_simulation所申请的资源总量
        device_simulation_config = task.deviceSimulation
        device_requirement = dict()
        for resource_request_config in device_simulation_config.resourceRequestDeviceSimulation:
            devices_list = list(resource_request_config.deviceResourceRequest)
            nums_list = list(resource_request_config.numResourceRequest)
            for device_index, device_type in enumerate(devices_list):
                num_device_type = int(device_requirement.get(device_type, 0)) + nums_list[device_index]
                device_requirement.update(
                    {f"{device_type}": num_device_type}
                )
        task_request.update({"device_simulation": {f"{task.userID}": device_requirement}})

        return task_request

    def check_resource_availability(self, task_request, available_resources):
        '''
        Example:
            task_request = {
                "logical_simulation": {"cpu": 1.0, "mem": 1.0},
                "device_simulation": {
                    "user_id1": {"high": 1, "low": 2}
                }
            }
            available_resources = {
                "logical_simulation": {"cpu": 3.0, "mem": 1.0},
                "device_simulation": {
                    "user_id1": {"high": 3, "low": 5},
                    "user_id2": {"middle": 10}
                }
            }
        '''
        # logical_simulation
        request_cpu, request_mem = \
            task_request.get("logical_simulation", {}).get("cpu", 0.0), \
            task_request.get("logical_simulation", {}).get("mem", 0.0),
        available_cpu, available_mem = \
            available_resources.get("logical_simulation", {}).get("cpu", 0.0), \
            available_resources.get("logical_simulation", {}).get("mem", 0.0),
        logical_availability = (request_cpu <= available_cpu) and\
                               (request_mem <= available_mem)
        if logical_availability == False:
            return False
        # device_simulation
        device_userid_list = list(task_request.get("device_simulation", {}).keys())
        if len(device_userid_list) == 0:  # device_simulation为空
            device_availability = True
        else:  # task有唯一的user_id
            device_userid = device_userid_list[0]
            phone_type_list = list(task_request.get("device_simulation", {})
                                   .get(device_userid, {}))
            device_availability = True
            for phone_type in phone_type_list:
                request_phonenum, available_phonenum = \
                    task_request.get("device_simulation", {})\
                        .get(device_userid, {})\
                        .get(phone_type, 0), \
                    available_resources.get("device_simulation", {})\
                        .get(device_userid, {})\
                        .get(phone_type, 0)
                device_availability = device_availability and\
                                      (request_phonenum <= available_phonenum)
        return (logical_availability and device_availability)

    def schedule_task(self, waiting_schedule_list):
        # 随便先写一个版本
        list_length = len(waiting_schedule_list)
        # 入队列优先分数
        time_score_list = [(list_length - i)/list_length for i in range(list_length)]
        # 优先级设定的分数
        priority_score_list = [waiting_schedule_list[i]["task_priority"]/10 for i in range(list_length)]
        # 计算调度分数
        schedule_score = [time_score + priority_score for time_score, priority_score in zip(time_score_list, priority_score_list)]
        # 获取分数最大的索引位置
        scheduled_task_index = schedule_score.index(max(schedule_score))
        return scheduled_task_index

    def schedule_next_task(self, task_queue: List[Any],
                           available_resources: Dict[str, Any]) -> Any:
        waiting_schedule_list = []
        for idx in range(len(task_queue)):
            task = task_queue[idx]
            task_priority = task.target.priority
            task_request = self.get_task_request_resource(task)
            if self.check_resource_availability(task_request, available_resources): #看现有资源是否支持
                waiting_schedule_list.append({
                    "task": task,
                    "task_priority": task_priority,
                    "task_request": task_request
                })
        if len(waiting_schedule_list) == 0:
            logger.info(task_id="", system_name="TaskMgr", module_name="task_scheduler",
                        message=f"[schedule_next_task]: There are not enough"
                                f" resources for all submitted tasks!")
            return None
        else:
            scheduled_task_index = self.schedule_task(waiting_schedule_list = waiting_schedule_list)
            task_scheduler_res = TaskSchedulerRes(
                task = waiting_schedule_list[scheduled_task_index]["task"],
                task_request = waiting_schedule_list[scheduled_task_index]["task_request"],
            )
        # print(f"task_scheduler_res.task_request = \n{task_scheduler_res.task_request}\n")
        return task_scheduler_res

class StrategyFactory:
    @staticmethod
    def create_strategy(strategy=None):
        return DefaultStrategy()