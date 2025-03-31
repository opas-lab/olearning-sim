import grpc
from ols.proto import resourceService_pb2 as resourceService__pb2
from ols.proto.resourceService_pb2_grpc import ResourceMgrServicer
import configparser
import os
import ray
#import logging
from ols.resourceMgr.res_param import ResourceDetail, Resource
from typing import List
import datetime
from ols.simu_log import Logger
from ols.resourceMgr.utils.utils import ResTableRepo
from ols.proto import taskService_pb2 as taskService__pb2
from ols.proto import phoneMgr_pb2 as phoneMgr__pb2
from ols.proto import phoneMgr_pb2_grpc


class ResourceManager(ResourceMgrServicer):
    def __init__(self):
        self._config = dict()
        self._ray_address = ""
        self._cpu = 0.0
        self._mem = 0.0
        self._phonemgr = ""
        self._logger = Logger()
        self.getConfig()
        self._resourceDetails = []

    def getConfig(self):
        currentPath = os.path.abspath(os.path.dirname(__file__))
        parentPath = os.path.abspath(os.path.join(currentPath, '..'))
        # 1. set log
        logPath = parentPath + "/config/repo_log.yaml"
        if not os.path.exists(logPath):
            raise Exception(f"logPath:{logPath} is not exist")
        self._logger.set_logger(log_yaml = logPath)
        # 2. read config file
        configPath = os.path.abspath(os.path.join(parentPath, './config/config.conf'))
        self._config = configparser.ConfigParser()
        if not os.path.exists(configPath):
            self._logger.error(task_id = "", system_name = "ResourceMgr", module_name = "resource_manager",
                message = f"[getConfig]: configPath:{configPath} is not exist.")               
            raise Exception(f"configPath:{configPath} is not exist")
        self._config.read(configPath)
        self._ray_address = f'{self._config.get("ray", "gcs_ip")[1:-1]}:{self._config.get("ray", "gcs_port")}'
        self._logger.info(task_id = "", system_name = "ResourceMgr", module_name = "resource_manager",
            message = f"[getConfig]: ray address: {self._ray_address}.")        
        # 3. get total resource from ray
        if ray.is_initialized() == False:
            ray.init("ray://" + self._ray_address)
        cluster_res = ray.cluster_resources()
        # 4. set ray cpu and mem
        self._cpu = round(float(cluster_res["CPU"]), 2)                  # uint : cores
        self._mem = round(float(cluster_res["memory"]) / 1000000000, 2)  # unit : G
        self._logger.info(task_id = "", system_name = "ResourceMgr", module_name = "resource_manager",
            message = f"[getConfig]: cluster_res: cpu:{self._cpu}, mem:{self._mem}.")
        # 5. init db
        try:
            self._res_repo = ResTableRepo(yaml_path = parentPath + "/config/resmgr_table.yaml")
        except Exception as e:
            self._logger.error(task_id="", system_name="ResMgr", module_name="resource_manager",
                         message=f"[getConfig]: Initiate self._task_repo failed, because of {e}")
            raise e
        # 6. phonemgr addres
        self._phonemgr = f'{self._config.get("grpc", "phonemgr_ip")[1:-1]}:' \
            f'{self._config.get("grpc", "phonemgr_port")}'
        self._logger.info(task_id = "", system_name = "ResourceMgr", module_name = "resource_manager",
            message = f"[getConfig]: phonemgr:{self._phonemgr}.")



    # 获取当前在整在使用的资源
    def get_current_res(self):
        # 1. get current resource by status == 0 (0:running)
        df = self._res_repo.get_data_by_item_values(item="status", values=[0])
        return df

    # 获取正在使用的cpu 和 mem 资源
    def get_remain_res(self):
        cpu = 0.0
        mem = 0.0
        # 1. sum of current running resource
        df = self.get_current_res()
        try:
            for i in range(df.shape[0]):
                cpu = cpu + df.iloc[i]["cpu"]
                mem = mem + df.iloc[i]["mem"]
        except Exception as e:
            self._logger.error(task_id="", system_name="ResMgr", module_name="res_manager",
                message=f"[get_remain_res]: get remain res failed, because of {e}")
        # 2. caculate the remain resource
        remain_res = Resource(
            cpu = self._cpu - cpu,
            mem = self._mem - mem)
        return remain_res

    # 服务端资源当前可用资源
    def getClusterAvailableResource(self, request, context):
        # 1. get remain res
        remain_res = self.get_remain_res()
        # 2. for res message
        message = resourceService__pb2.ServerClusterResource(
          cores = remain_res.cpu,
          mem = remain_res.mem
        )
        return message

    def getDetailResbyTaskId(self, task_id):
        # 通过数据库进行校验 task_id是否存在 并且状态为running
        # 1. get res by taskid
        df = self._res_repo.get_data_by_item_values(item="task_id", values=[task_id])
        # 2. choose the running one (status 0:running)
        for i in range(df.shape[0]):
            try:
                status = df.iloc[i]["status"]
                if status == 0:
                    res = ResourceDetail(
                        task_id = df.iloc[i]["task_id"],
                        cpu     = df.iloc[i]["cpu"],
                        mem     = df.iloc[i]["mem"],
                        status  = df.iloc[i]["status"],
                        freeze_time = df.iloc[i]["freeze_time"],
                        release_time = df.iloc[i]["release_time"])
                    return res
            except Exception as e:
                self._logger.error(task_id=task_id, system_name="ResMgr", module_name="res_manager",
                             message=f"[getDetailResbyTaskId]: get detail by task_id={task_id}  failed, because of {e}")
        return None    

    # 服务端资源申请接口
    # task_id: 需要申请资源的taskid (str)
    # cpu : 申请的cpu资源 (float)
    # mem : 申请的mem资源 (float)
    # return : true :申请成功，false:申请失败
    def requestClusterResource(self, request, context):
        try:
            # 1. check param
            if request.taskId == "":
                self._logger.error(task_id = "", system_name = "ResourceMgr", module_name = "resource_manager",
                    message = f"[requestClusterResource]: {request.taskId} request \
                    server resource fail,taskid is null.")
                return resourceService__pb2.ServerClusterStatus(
                        status = resourceService__pb2.ServerClusterResStatus.TASKID_NULL)
            if float(request.cores) < 0 or float(request.mem) < 0:
                self._logger.error(task_id = request.taskId, system_name = "ResourceMgr", module_name = "resource_manager",
                    message = f"[requestClusterResource]: {request.taskId} request \
                    server resource fail, illegal value of cpu:{request.cores}, mem:{request.mem}.")
                return resourceService__pb2.ServerClusterStatus(
                        status = resourceService__pb2.ServerClusterResStatus.FAIL)
            # 2. read remain_res
            data = self.get_remain_res()
            remain_cpu = data.cpu;
            remain_mem = data.mem;
            # 3. check res
            if float(request.cores) >  float(remain_cpu) or float(request.mem) > float(remain_mem):
                self._logger.error(task_id = request.taskId, system_name = "ResourceMgr", module_name = "resource_manager",
                    message = f"[requestClusterResource]: {request.taskId} request \
                    server resource fail, not enough res.")
                return resourceService__pb2.ServerClusterStatus(
                        status = resourceService__pb2.ServerClusterResStatus.NOT_ENOUGH_RES)
            # 2. check task_id
            if self.getDetailResbyTaskId(request.taskId) is not None:
                self._logger.error(task_id = request.taskId, system_name = "ResourceMgr", module_name = "resource_manager",
                    message = f"[requestClusterResource]: current task_id \
                    {request.taskId} has already request res!")
                return resourceService__pb2.ServerClusterStatus(
                        status = resourceService__pb2.ServerClusterResStatus.TASK_REPEAT)
            # 3. 分配资源并 将资源使用信息写入数据库
            remain_cpu = float(remain_cpu) - float(request.cores)
            remain_mem = float(remain_mem) - float(request.mem)
            data = {
                "task_id"     : request.taskId,
                "cpu"         : request.cores,
                "mem"         : request.mem,
                "status"      : 0,
                "freeze_time" : datetime.datetime.now()}
            ok = self._res_repo.insert_data(data)
            if not ok :
                self._logger.error(task_id = request.taskId, system_name = "ResourceMgr", module_name = "resource_manager",
                    message = f"[requestClusterResource]:request resource fail because of insert data fail")
                return resourceService__pb2.ServerClusterStatus(
                    status = resourceService__pb2.ServerClusterResStatus.FAIL)                
            # 4. record success log
            self._logger.info(task_id = request.taskId, system_name = "ResourceMgr", module_name = "resource_manager",
                message = f"[requestClusterResource]:{request.taskId} request resouce \
                cpu:{request.cores},mem:{request.mem} success! current available resource \
                is cpu:{remain_cpu} mem:{remain_mem}")            
            return resourceService__pb2.ServerClusterStatus(
                    status = resourceService__pb2.ServerClusterResStatus.SUCCESS)  
        except Exception as e:
            self._logger.error(task_id = request.taskId, system_name = "ResourceMgr", module_name = "resource_manager",
                message = f"[requestClusterResource]:request resource fail because of {e}")
            return resourceService__pb2.ServerClusterStatus(
                status = resourceService__pb2.ServerClusterResStatus.FAIL)

    # 释放资源
    # task_id: 需要释放资源的task id (str)
    # return : true :申请成功，false:申请失败
    def releaseClusterResource(self, request, context):
        try:
            # 1. check param
            res_detail = self.getDetailResbyTaskId(request.taskId)
            if res_detail is None:
                self._logger.error(task_id = request.taskId, system_name = "ResourceMgr", module_name = "resource_manager",
                    message = f"[releaseClusterResource]: current task \
                    {request.taskId} not found!l")
                return resourceService__pb2.ServerClusterStatus(
                        status = resourceService__pb2.ServerClusterResStatus.TASKID_NOT_FOUND)
            # 2. delete res by taskid
            ok = self._res_repo.delete_by_taskid(request.taskId)
            if not ok :
                self._logger.error(task_id = request.taskId, system_name = "ResourceMgr", module_name = "resource_manager",
                    message = f"[releaseClusterResource]:release resource fail because of delete fail")
                return resourceService__pb2.ServerClusterStatus(
                        status = resourceService__pb2.ServerClusterResStatus.FAIL)                
            # 3. read remain_res and record success log
            item = self.get_remain_res()
            remain_cpu = item.cpu
            remain_mem = item.mem
            self._logger.info(task_id = request.taskId, system_name = "ResourceMgr", module_name = "resource_manager",
                message = f"[releaseClusterResource]:{request.taskId} release resource \
                cpu:{res_detail.cpu},mem:{res_detail.mem} success! current available resource \
                is cpu:{remain_cpu} mem:{remain_mem}")
            return resourceService__pb2.ServerClusterStatus(
                    status = resourceService__pb2.ServerClusterResStatus.SUCCESS)
        except Exception as e:
            self._logger.error(task_id = request.taskId, system_name = "ResourceMgr", module_name = "resource_manager",
                message = f"[releaseClusterResource]:release resource fail because of {e}")
            return resourceService__pb2.ServerClusterStatus(
                    status = resourceService__pb2.ServerClusterResStatus.FAIL) 

    # 查询服务端资源占用明细
    # return str
    def getClusterResourceDetail(self, request, context):
        # 1. get current resource by status == 0 (0:running) in json list
        current_res = self._res_repo.get_jsons_by_item_values(item="status", values=[0])
        self._logger.info(task_id = "", system_name = "ResourceMgr", module_name = "resource_manager",
            message = f"[getClusterResourceDetail]: details: {current_res}")
        # 2. for message
        msg = resourceService__pb2.ServerClusterDetail(
            detail = f"detail:{current_res}"
        )
        return msg

    def getClusterTotalResource(self, request, context):
        # 1. for message
        message = resourceService__pb2.ServerClusterResource(
          cores = self._cpu,
          mem = self._mem
        )   
        return message

    def getPhoneResource(self, request, context):
        pass

    def getTotalResource(self, request, context):
        pass

    def getVMResource(self, request, context):
        pass

    def getResource(self, request, context):
        # 1. get cluster res
        clusterRes = self.getClusterAvailableResource(request, context)
        phone_resource = phoneMgr__pb2.AllUsersDeviceAvailableResource()
        # 2. get phone res
        try:
            with grpc.insecure_channel(self._phonemgr) as channel:
                stub = phoneMgr_pb2_grpc.TaskManagerStub(channel)
                phone_resource = stub.getDeviceAvailableResource(
                    phoneMgr__pb2.google_dot_protobuf_dot_empty__pb2.Empty()
                )
        except Exception as e:
            self._logger.error(task_id="", system_name="ResourceMgr", module_name="ResourceMgr",
                         message=f"[get_available_resources]: get_available_resources from PhoneMgr failed, because of {e}")
        # 3. collect return
        res = resourceService__pb2.Resource (
            clusterRes = clusterRes,
            phoneRes = phone_resource
        )
        return res

    def requestResource(self, request, context):
        # 1. default return value give FAIL or 0
        clusterStatus = resourceService__pb2.ServerClusterStatus(
                        status = resourceService__pb2.ServerClusterResStatus.FAIL)
        phoneStatus = phoneMgr__pb2.ActionStatus(
                        isSuccess = 0)
        # 2. request cluster res (requestType 0:cluster adn phone ,1:cluster,2:phone)
        if (request.type == 0 or request.type == 1) and request.clusterReq != None:
            clusterStatus = self.requestClusterResource(request.clusterReq, context)
        # 3. reuqest phone res
        if (request.type == 0 or request.type == 2) and request.phoneReq != None:
            try:
                with grpc.insecure_channel(self._phonemgr) as channel:
                    stub = phoneMgr_pb2_grpc.TaskManagerStub(channel)
                    phoneStatus = stub.requestDeviceResource(request.phoneReq)
            except Exception as e:
                self._logger.error(task_id=request.phoneReq.taskID, system_name="ResourceMgr", module_name="ResourceMgr",
                             message=f"[freeze]: freeze resource of task={request.phoneReq.taskID} in phonemgr failed, because of {e}")
        # 4. return result
        status = resourceService__pb2.RequestStatus (
            clusterStatus = clusterStatus,
            phoneStatus = phoneStatus
        )
        return status

    def releaseResource(self, request, context):
        # 1. default return value give FAIL or 0
        clusterStatus = resourceService__pb2.ServerClusterStatus(
                        status = resourceService__pb2.ServerClusterResStatus.FAIL)
        phoneStatus = phoneMgr__pb2.ActionStatus(
                        isSuccess = 0)
        # 1. release cluster res
        if (request.type == 0 or request.type == 1) and request.taskId != None:
            clusterStatus = self.releaseClusterResource(request, context)
        # 2. release phone res
        if (request.type == 0 or request.type == 2) and request.taskId != None:
            try:
                with grpc.insecure_channel(self._phonemgr) as channel:
                    stub = phoneMgr_pb2_grpc.TaskManagerStub(channel)
                    taskID = taskService__pb2.TaskID(taskID=request.taskId)
                    phoneStatus = stub.releaseDeviceResource(taskID)
            except Exception as e:
                self._logger.error(task_id = request.taskId, system_name = "ResourceMgr", module_name = "ResouceMgr",
                    message=f"[release]: release resource of task={request.taskId} in phonemgr failed, because of {e}")
        # 3. return result
        status = resourceService__pb2.ReleaseStatus (
            clusterStatus = clusterStatus,
            phoneStatus = phoneStatus
        )
        return status

