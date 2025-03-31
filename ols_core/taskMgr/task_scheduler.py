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
    def __init__(self, resmgr="", strategy=None):
        self._resmgr = resmgr
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

        try:
            with grpc.insecure_channel(self._resmgr) as channel:
                stub = resourceService_pb2_grpc.ResourceMgrStub(channel)
                resource = stub.getResource(
                    resourceService__pb2.google_dot_protobuf_dot_empty__pb2.Empty())
                logical_resource = resource.clusterRes
                phone_resource = resource.phoneRes
                # logical simulation
                available_resources.update({
                    "logical_simulation": {
                        "cpu": logical_resource.cores,
                        "mem": logical_resource.mem
                    }
                })
                # device_simulation
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
                         message=f"[get_available_resources]: getResource from ResourceMgr failed, because of {e}")
            available_resources.update({
                "logical_simulation": {"cpu": 0.0, "mem": 0.0},
                "device_simulation": {}
            })

        return available_resources

    def freeze(self, task, task_request):
        # type = 0:cluster and phone, 1:only cluster, 2:only phone

        logical_request, device_request = False, False
        logical_freeze_flag, device_freeze_flag = False, False

        # 确定freeze_type
        # logical simulation
        if task.logicalSimulation.resourceRequestLogicalSimulation:
            # 判断是否有使用设备
            logical_request_sum = 0
            for resource_request in task.logicalSimulation.resourceRequestLogicalSimulation:
                logical_request_sum += sum(list(resource_request.numResourceRequest))
            if logical_request_sum > 0:
                logical_request = True
            else:
                logical_freeze_flag = True
        else: #无logical资源需求
            logical_freeze_flag = True
        # device simulation
        if task.deviceSimulation.resourceRequestDeviceSimulation:
            # 判断是否有使用设备
            device_request_sum = 0
            for resource_request in task.deviceSimulation.resourceRequestDeviceSimulation:
                device_request_sum += sum(list(resource_request.numResourceRequest))
            if device_request_sum > 0:
                device_request = True
            else:
                device_freeze_flag = True
        else: #无device资源需求
            device_freeze_flag = True

        # freeze_type
        if logical_request and device_request:
            freeze_type = 0
        elif logical_request and device_request == False:
            freeze_type = 1
        elif logical_request == False and device_request:
            freeze_type = 2
        else:
            return False

        # parameters
        # initiation
        logical_req_param = resourceService__pb2.ServerClusterReq(taskId=task.taskID.taskID, cores=0.0, mem=0.0)
        device_req_param = phoneMgr__pb2.DeviceResource(taskID=task.taskID.taskID, userID=task.userID, deviceResourceInfo=[])
        # logical_req_param
        if freeze_type == 0 or freeze_type == 1:
            logical_req_param = resourceService__pb2.ServerClusterReq(
                taskId=task.taskID.taskID,
                cores=task_request.get("logical_simulation", {}).get("cpu", 0.0),
                mem=task_request.get("logical_simulation", {}).get("mem", 0.0)
            )
        # device_req_param
        if freeze_type == 0 or freeze_type == 2:
            device_resource_info = []
            device_request = task_request.get("device_simulation", {}).get(task.userID, {})
            for device_type in device_request:
                device_resource_info.append((
                    phoneMgr__pb2.DeviceResourceInfo(
                        phoneType=device_type,
                        num=device_request.get(device_type, 0)
                    )
                ))
            device_req_param = phoneMgr__pb2.DeviceResource(
                taskID=task.taskID.taskID,
                userID=task.userID,
                deviceResourceInfo=device_resource_info
            )

        # freeze
        try:
            with grpc.insecure_channel(self._resmgr) as channel:
                stub = resourceService_pb2_grpc.ResourceMgrStub(channel)
                req_params = resourceService__pb2.ResReq(
                    type = freeze_type,
                    clusterReq = logical_req_param,
                    phoneReq = device_req_param
                )
                response = stub.requestResource(req_params)
            logical_response = response.clusterStatus
            device_response = response.phoneStatus
            # 状态判断
            if freeze_type == 0 or freeze_type == 1: #有逻辑仿真
                if logical_response.status == 0:
                    logical_freeze_flag = True
                else:
                    logger.error(task_id=task.taskID.taskID, system_name="TaskMgr", module_name="task_scheduler",
                                 message=f"[freeze]: freeze logical resource of task={task.taskID.taskID} failed, error = {resourceService__pb2.ServerClusterResStatus.Name(logical_response.status)}")
                    logical_freeze_flag = False
            if freeze_type == 0 or freeze_type == 2: #有真机仿真
                if device_response.isSuccess == True:
                    device_freeze_flag = True
                else:
                    logger.error(task_id=task.taskID.taskID, system_name="TaskMgr", module_name="task_scheduler",
                                 message=f"[freeze]: freeze device resource of task={task.taskID.taskID} in phonemgr failed.")
                    device_freeze_flag = False

            return logical_freeze_flag and device_freeze_flag

        except Exception as e:
            logger.error(task_id=task.taskID.taskID, system_name="TaskMgr", module_name="task_scheduler",
                         message=f"[freeze]: freeze resources of task={task.taskID.taskID} failed because of {e}")
            return False


    def release(self, task):
        # type = 0:cluster and phone, 1:only cluster, 2:only phone

        logical_release, device_release = False, False
        logical_release_flag, device_release_flag = False, False

        # 确定release_type
        # logical simulation
        if task.logicalSimulation.resourceRequestLogicalSimulation:
            # 判断是否有使用设备
            logical_request_sum = 0
            for resource_request in task.logicalSimulation.resourceRequestLogicalSimulation:
                logical_request_sum += sum(list(resource_request.numResourceRequest))
            if logical_request_sum > 0:
                logical_release = True
            else:
                logical_release_flag = True
        else: #无logical资源需求
            logical_release_flag = True
        # device simulation
        if task.deviceSimulation.resourceRequestDeviceSimulation:
            # 判断是否有使用设备
            device_request_sum = 0
            for resource_request in task.deviceSimulation.resourceRequestDeviceSimulation:
                device_request_sum += sum(list(resource_request.numResourceRequest))
            if device_request_sum > 0:
                device_release = True
            else:
                device_release_flag = True
        else: #无device资源需求
            device_release_flag = True


        # release_type
        if logical_release and device_release:
            release_type = 0
        elif logical_release and device_release == False:
            release_type = 1
        elif logical_release == False and device_release:
            release_type = 2
        else:
            return False

        # release
        try:
            with grpc.insecure_channel(self._resmgr) as channel:
                stub = resourceService_pb2_grpc.ResourceMgrStub(channel)
                release_param = resourceService__pb2.ResRelease(
                    type=release_type,
                    taskId=task.taskID.taskID
                )
                response = stub.releaseResource(release_param)
            logical_response = response.clusterStatus
            device_response = response.phoneStatus
            # 状态判断
            if release_type == 0 or release_type == 1:  # 有逻辑仿真
                if logical_response.status == 0:
                    logical_release_flag = True
                else:
                    logger.error(task_id=task.taskID.taskID, system_name="TaskMgr", module_name="task_scheduler",
                                 message=f"[release]: release logical resource of task={task.taskID.taskID} failed, error = {resourceService__pb2.ServerClusterResStatus.Name(logical_response.status)}")
                    logical_release_flag = False
            if release_type == 0 or release_type == 2:  # 有真机仿真
                if device_response.isSuccess == True:
                    device_release_flag = True
                else:
                    logger.error(task_id=task.taskID.taskID, system_name="TaskMgr", module_name="task_scheduler",
                                 message=f"[release]: release device resource of task={task.taskID.taskID} in phonemgr failed.")
                    device_release_flag = False

            return logical_release_flag and device_release_flag

        except Exception as e:
            logger.error(task_id=task.taskID.taskID, system_name="TaskMgr", module_name="task_scheduler",
                         message=f"[release]: release resources of task={task.taskID.taskID} failed because of {e}")
            return False


    def run(self, task_queue):
        available_resources = self.get_available_resources() #查询逻辑仿真、真机可用资源
        task_scheduler_res = self.strategy.schedule_next_task(
            task_queue=task_queue,
            available_resources=available_resources
        ) #调度得到1个可用的任务
        return task_scheduler_res
