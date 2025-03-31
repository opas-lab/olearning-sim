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
            available_resources (Dict[str, Any]): The tatal available resources.
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
        # logical_simulation
        operators = task.logicalSimulation.operators
        cpu_max, mem_max = 0.0, 0.0
        for operator in operators:
            cpu_max = max(operator.simuParams.resourceRequest.cpu,
                          cpu_max)
            mem_max = max(operator.simuParams.resourceRequest.mem,
                          mem_max)
        cpu_max = max(float(int(cpu_max)+1) if cpu_max>int(cpu_max) else float(int(cpu_max)), 1.0) #向上取整，最小为1.0
        task_request.update({"logical_simulation": {"cpu": cpu_max,
                                                    "mem": mem_max}})
        # device_simulation
        device_requirement = dict()
        for device_simulation_requirement in task.deviceSimulation.deviceRequirement:
            device_requirement.update({
                f"{device_simulation_requirement.phoneType}": device_simulation_requirement.num
            })
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

    def schedule_next_task(self, task_queue: List[Any],
                           available_resources: Dict[str, Any]) -> Any:
        # check task in task_queue
        for idx in range(len(task_queue)):
            task = task_queue[idx]
            task_request = self.get_task_request_resource(task)
            if self.check_resource_availability(task_request=task_request,
                                                available_resources=available_resources):
                task_scheduler_res = TaskSchedulerRes(
                    task = task,
                    task_request = task_request
                )
                return task_scheduler_res
        logger.info(task_id="", system_name="TaskMgr", module_name="task_scheduler",
                    message=f"[schedule_next_task]: There are not enough"
                            f" resources for all submitted tasks!")
        return None

class StrategyFactory:
    @staticmethod
    def create_strategy(strategy=None):
        return DefaultStrategy()