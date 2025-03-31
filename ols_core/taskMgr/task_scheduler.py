import os
import grpc
from ols.simu_log import Logger
from ols.proto import taskService_pb2 as taskService__pb2
from ols.proto import resourceService_pb2 as resourceService__pb2
from ols.proto import resourceService_pb2_grpc
from ols.taskMgr.utils.scheduler_strategy import StrategyFactory
from ols.proto import phoneMgr_pb2 as phoneMgr__pb2
from ols.proto import phoneMgr_pb2_grpc

# filepath
FILEPATH = os.path.abspath(os.path.dirname(__file__))
# log
logger = Logger()
logger.set_logger(log_yaml=os.path.abspath(os.path.join(FILEPATH, '..'))+"/config/repo_log.yaml")

class TaskScheduler:
    def __init__(self, resmgr="", phonemgr="", strategy=None):
        self._resmgr = resmgr
        self._phonemgr = phonemgr
        self.strategy = StrategyFactory.create_strategy(strategy)

    def get_available_resources(self):
        '''
        Example:
            available_resources = {
                "logical_simulation": {"cpu": 3.0, "mem": 1.0},
                "device_simulation": {
                    "user_id1": {"high": 3, "low": 5},
                    "user_id2": {"middle": 10}
                }
            }
        '''
        available_resources = dict()
        # logical simulation
        try:
            with grpc.insecure_channel(self._resmgr) as channel:
                stub = resourceService_pb2_grpc.ResourceMgrStub(channel)
                logical_resource = stub.getServerResource(
                    resourceService__pb2.google_dot_protobuf_dot_empty__pb2.Empty())
            available_resources.update({
                "logical_simulation":{
                    "cpu": logical_resource.cores,
                    "mem": logical_resource.mem
                }
            })
        except Exception as e:
            logger.error(task_id="", system_name="TaskMgr", module_name="task_scheduler",
                         message=f"[get_available_resources]: get_available_resources from ResourceMgr failed, because of {e}")
            available_resources.update({
                "logical_simulation": {"cpu": 0.0, "mem": 0.0}
            })
        # device_simulation
        try:
            with grpc.insecure_channel(self._phonemgr) as channel:
                stub = phoneMgr_pb2_grpc.TaskManagerStub(channel)
                phone_resource = stub.getDeviceAvailableResource(
                    phoneMgr__pb2.google_dot_protobuf_dot_empty__pb2.Empty()
                )
            user_resource_dict = dict()
            for user_resource in phone_resource.userDeviceAvailableResource:
                user_id = user_resource.userID
                device_resource_dict = dict()
                device_resource_infos = user_resource.deviceResourceInfo
                for device_resource_info in device_resource_infos:
                    device_resource_dict.update({
                        f"{device_resource_info.phoneType}": device_resource_info.num
                    })
                user_resource_dict.update({user_id: device_resource_dict})
            available_resources.update({"device_simulation": user_resource_dict})
        except Exception as e:
            logger.error(task_id="", system_name="TaskMgr", module_name="task_scheduler",
                         message=f"[get_available_resources]: get_available_resources from PhoneMgr failed, because of {e}")
            available_resources.update({"device_simulation": {}})

        return available_resources

    def freeze(self, task, task_request):
        # logical_simulation
        if task.logicalSimulation.operators:
            try:
                with grpc.insecure_channel(self._resmgr) as channel:
                    stub = resourceService_pb2_grpc.ResourceMgrStub(channel)
                    req_param = resourceService__pb2.ServerClusterReq(
                        task_id=task.taskID.taskID,
                        cores=task_request.get("logical_simulation", {}).get("cpu", 0.0),
                        mem=task_request.get("logical_simulation", {}).get("mem", 0.0)
                    )
                    response = stub.requestServerResource(req_param)
                if response.status == 0:
                    logical_freeze_flag = True
                else:
                    logger.error(task_id=task.taskID.taskID, system_name="TaskMgr", module_name="task_scheduler",
                                 message=f"[freeze]: freeze resource of task={task.taskID.taskID} in resmgr failed, error = {resourceService__pb2.ServerClusterResRequstStatus.Name(response.status)}")
                    logical_freeze_flag = False
            except Exception as e:
                logger.error(task_id=task.taskID.taskID, system_name="TaskMgr", module_name="task_scheduler",
                            message=f"[freeze]: freeze resource of task={task.taskID.taskID} failed because of {e}")
                logical_freeze_flag = False
        else: #无logical资源需求
            logical_freeze_flag = True
        # device_simulation
        if task.deviceSimulation.deviceRequirement: #如果有device资源需求
            try:
                with grpc.insecure_channel(self._phonemgr) as channel:
                    stub = phoneMgr_pb2_grpc.TaskManagerStub(channel)
                    device_resource_info = []
                    for device_requirement in task.deviceSimulation.deviceRequirement:
                        device_resource_info.append((
                            phoneMgr__pb2.DeviceResourceInfo(
                                phoneType = device_requirement.phoneType,
                                num = device_requirement.num
                            )
                        ))
                    req_param = phoneMgr__pb2.DeviceResource(
                        taskID = task.taskID.taskID,
                        userID = task.userID,
                        deviceResourceInfo = device_resource_info
                    )
                    response = stub.requestDeviceResource(req_param)
                if response.isSuccess == True:
                    device_freeze_flag = True
                else:
                    logger.error(task_id=task.taskID.taskID, system_name="TaskMgr", module_name="task_scheduler",
                                 message=f"[freeze]: freeze resource of task={task.taskID.taskID} in phonemgr failed.")
                    device_freeze_flag = False
            except Exception as e:
                logger.error(task_id=task.taskID.taskID, system_name="TaskMgr", module_name="task_scheduler",
                             message=f"[freeze]: freeze resource of task={task.taskID.taskID} in phonemgr failed, because of {e}")
                device_freeze_flag = False
        else: #无device资源需求
            device_freeze_flag = True
        return logical_freeze_flag and device_freeze_flag

    def release(self, task):
        task_id = task.taskID.taskID
        # logical_simulation
        if task.logicalSimulation.operators:
            try:
                with grpc.insecure_channel(self._resmgr) as channel:
                    stub = resourceService_pb2_grpc.ResourceMgrStub(channel)
                    release_param = resourceService__pb2.ServerClusterReq(
                        task_id = task_id,
                        cores = 0,
                        mem = 0
                    )
                    response = stub.releaseServerResource(release_param)
                if response.status == 0 or response.status == 4:
                    logical_release_flag = True
                else:
                    logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_scheduler",
                                 message=f"[release]: release resource of task={task_id} in resmgr failed, error = {resourceService__pb2.ServerClusterResRequstStatus.Name(response.status)}")
                    logical_release_flag = False
            except Exception as e:
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_scheduler",
                             message=f"[release]: release resource of task={task_id} failed because of {e}")
                logical_release_flag = False
        else:  # 无logical资源释放需求
            logical_release_flag = True
        # device_simulation
        if task.deviceSimulation.deviceRequirement:  # 如果有device资源需求
            try:
                with grpc.insecure_channel(self._phonemgr) as channel:
                    stub = phoneMgr_pb2_grpc.TaskManagerStub(channel)
                    taskID = taskService__pb2.TaskID(taskID=task_id)
                    response = stub.releaseDeviceResource(taskID)
                if response.isSuccess == True:
                    device_release_flag = True
                else:
                    logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_scheduler",
                                 message=f"[release]: release resource of task={task_id} in phonemgr failed.")
                    device_release_flag = False
            except Exception as e:
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_scheduler",
                             message=f"[release]: release resource of task={task_id} in phonemgr failed, because of {e}")
                device_release_flag = False
        else:  # 无device资源需求
            device_release_flag = True
        return logical_release_flag and device_release_flag

    def run(self, task_queue):
        available_resources = self.get_available_resources() #查询逻辑仿真、真机可用资源
        task_scheduler_res = self.strategy.schedule_next_task(
            task_queue=task_queue,
            available_resources=available_resources
        ) #调度得到1个可用的任务
        return task_scheduler_res