"""The Python implementation of the GRPC deviceflow service."""
from concurrent import futures
import grpc
import logging
import threading
import time
import os
import yaml
import json

from ols.proto import deviceflow_pb2_grpc, deviceflow_pb2
from ols.proto.deviceflow_pb2_grpc import TaskOperatorOrientedDeviceFlowServicer
from ols.utils.repo_utils import SqlDataBase
from ols.simu_log import Logger

from ols.deviceflow.utils.config_parser import get_source_url, get_source_topic, get_shelfroom_url, \
    get_shelfroom_namespace, get_port, get_max_workers

from ols.deviceflow.non_grpc.registry import TaskOrientedDeviceFlowRegistry

from ols.deviceflow.non_grpc.deviceflow import DeviceFlow
from ols.deviceflow.utils.validate_parameters import ValidateParameters
from ols.deviceflow.utils.validate_parameters import ComputeResource

from ols.deviceflow.non_grpc.strategy import Strategy
from ols.deviceflow.non_grpc.sorter import Sorter
from ols.deviceflow.non_grpc.dispatcher import Dispatcher

import threading
import concurrent.futures

# filepath
FILEPATH = os.path.abspath(os.path.dirname(__file__))              # deviceflow_server.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '../..'))         # ols所在文件夹的绝对路径
DEVICEFLOW_TABLE_PATH = OLSPATH + "/config/deviceflow_table.yaml"  # device_table_path
DEVICEFLOW_CONFIG_PATH = OLSPATH + "/config/deviceflow_config.yaml"  # deviceflow_config_path
LOG_PATH = OLSPATH + "/config/repo_log.yaml"                       # log日志配置的文件路径
# log
logger = Logger()
logger.set_logger(log_yaml=LOG_PATH)


class DeviceFlowGrpcService(TaskOperatorOrientedDeviceFlowServicer):
    def __init__(
        self,
        inbound_pulsar_url: str,
        inbound_pulsar_topic: str,
        shelfroom_url: str,
        shelfroom_namespace: str,
    ):
        self._inbound_pulsar_url = inbound_pulsar_url
        self._inbound_pulsar_topic = inbound_pulsar_topic
        self._shelfroom_url = shelfroom_url
        self._shelfroom_namespace = shelfroom_namespace
        # initiate
        try:
            self._deviceflow_repo = SqlDataBase(yaml_path=DEVICEFLOW_TABLE_PATH)
        except Exception as e:
            logger.error(task_id="", system_name="Deviceflow", module_name="DeviceFlowGrpcService",
                         message=f"[__init__]: Initiate self._deviceflow_repo failed, because of {e}")
            raise e
        self._deviceflow = DeviceFlow(
            self._inbound_pulsar_url, self._inbound_pulsar_topic,
            self._shelfroom_url, self._shelfroom_namespace
        )
        self._sorter = Sorter(
            inbound_room=self._deviceflow._inbound_room,
            shelf_room=self._deviceflow._shelf_room
        )
        self._registry = {}
        self._flow = {}
        self._sorter_params = {}
        self._dispatcher = {}
        self.initiate_from_repo()
        ### run thread ###
        self._sort_thread = threading.Thread(target=self.sort)          # sort线程，用于接受、sort消息
        self._sort_thread.start()
        self._dispatch_thread = threading.Thread(target=self.dispatch)  # dispatch线程，用于按照可配置策略分发消息
        self._dispatch_thread.start()
        self._flow_release_thread = threading.Thread(target=self.flow_release)  # flow_release线程，清除已经完成dispatch的变量、数据库
        self._flow_release_thread.start()

    def initiate_from_repo(self):
        # 根据数据库初始化self._registry
        task_registry_df = self._deviceflow_repo.get_all_items_not_None(conditions=["task_id", "task_registry"])
        if task_registry_df.shape[0] > 0:
            for task_registry_idx in range(task_registry_df.shape[0]):
                task_id = task_registry_df.iloc[[task_registry_idx]]["task_id"].item()
                task_registry_item = task_registry_df.iloc[[task_registry_idx]]["task_registry"].item()
                if task_id not in self._registry:
                    try:
                        task_registry_item = json.loads(task_registry_item)
                        total_compute_resources = task_registry_item["total_compute_resources"]
                        if isinstance(task_registry_item["total_compute_resources"], list) and len(total_compute_resources) > 0 and \
                            set(total_compute_resources).issubset({"logical_simulation", "device_simulation"}):
                            self._registry.update({
                                task_id: {
                                    "total_compute_resources": list(total_compute_resources)
                                }
                            })
                    except:
                        continue
        print(f"[initiate_from_repo]: self._registry = {self._registry}")

        # 根据数据库初始化self._flow, self._sorter, self._dispatcher
        flow_df = self._deviceflow_repo.get_all_items_not_None(conditions=["task_id", "flow_id", "flow"])
        if flow_df.shape[0] > 0:
            for flow_idx in range(flow_df.shape[0]):
                flow_id = flow_df.iloc[[flow_idx]]["flow_id"].item()
                flow_params = flow_df.iloc[[flow_idx]]["flow"].item()
                # get self._flow
                try:
                    flow_params = json.loads(flow_params)
                    self._flow.update({
                        flow_id: flow_params
                    })
                except:
                    continue

                # get self._sorter
                try:
                    task_id = flow_params["task_id"]
                except:
                    continue
                if self._flow[flow_id]["to_sort"]:
                    if not self._deviceflow._shelf_room.has_shelf(flow_id):
                        self._deviceflow._shelf_room.add_shelf(flow_id)
                    self._sorter_params.update({
                        flow_id: {
                            "task_id": task_id,
                            "notify_status": self._flow[flow_id]["notify_start_called"]
                        }
                    })
                    self._sorter.update(self._sorter_params)

                # get self._dispatcher
                if self._flow[flow_id]["to_dispatch"]:
                    self._flow[flow_id]["to_dispatch"] = False #为了进入dispatch线程重新提交
                    is_update_flow = self._deviceflow.update_flow(flow_id, task_id, self._flow[flow_id])
                    if not is_update_flow:
                        continue
                    flow_dispatcher = Dispatcher(
                        task_id = task_id,
                        flow_id = flow_id,
                        shelf_room = self._deviceflow._shelf_room,
                        strategy = flow_params["strategy"],
                        outbound_service = flow_params["outbound_service"],
                    )
                    if Strategy.check_real_time_dispatch(strategy=self._flow[flow_id]["strategy"]):
                        # 如果是实时策略，则需要判断是否需要dispatcher
                        is_prepare_to_dispatch = self._deviceflow.check_all_notify_complete(
                            task_registry=self._registry[task_id],
                            flow_params=self._flow[flow_id]
                        )
                        if is_prepare_to_dispatch:
                            flow_dispatcher.release_dispatch()
                    else:

                        flow_dispatcher.release_dispatch()
                    self._dispatcher.update({flow_id: flow_dispatcher})

        print(f"[initiate_from_repo]: self._flow = {self._flow}")
        logger.info(task_id="", system_name="Deviceflow", module_name="INITIATE_FROM_REPO",
                     message=f"[initiate_from_repo]: self._registry = {self._registry}, self._flow = {self._flow}")


    def GetDeviceflowPulsarClient(self, request, context):
        with open(DEVICEFLOW_CONFIG_PATH, 'r') as file:
            deviceflow_config = yaml.load(file, Loader=yaml.FullLoader)
        pulsar_url = deviceflow_config["source_url"]
        pulsar_topic = deviceflow_config["source_topic"]
        return deviceflow_pb2.PulsarClient(url=pulsar_url, topic=pulsar_topic)


    def GetDeviceflowWebsocket(self, request, context):
        with open(DEVICEFLOW_CONFIG_PATH, 'r') as file:
            deviceflow_config = yaml.load(file, Loader=yaml.FullLoader)
        websocket_url = deviceflow_config["source_websocket_url"]
        return deviceflow_pb2.Websocket(url=websocket_url)


    def NotifyStart(self, request, context):
        # print(f"[NotifyStart]: request = \n{request}")
        task_id = request.task_id
        flow_id = request.routing_key
        validation = ValidateParameters()
        validation_result, validation_message = validation.check_params_of_notify_start(request)
        if not validation_result:
            logger.error(
                task_id=task_id, system_name="Deviceflow", module_name="NotifyStart",
                message=f"[notify_start]: ValidateParameters Failed because {validation_message}."
            )
            return deviceflow_pb2.OperationResponse(is_success=False)

        # 看是否有注册，如无注册则返回失败
        if not self._registry.get(task_id):
            logger.error(
                task_id=task_id, system_name="Deviceflow", module_name="NotifyStart",
                message=f"[notify_start]: Failed to start deviceflow because task_id = {task_id} is not registered."
            )
            return deviceflow_pb2.OperationResponse(is_success=False)

        is_success, flow_params = self._deviceflow.notify_start(self._flow, request)
        if not is_success: # 如果notify_start失败了
            logger.error(
                task_id=task_id, system_name="Deviceflow", module_name="NotifyStart",
                message=f"[notify_start]: Failed to notify_start deviceflow of flow_id = {flow_id}, task_id = {task_id}."
            )
            is_stop = self.stop(task_id=task_id, flow_id=flow_id, delete_flow_id=True)
            if not is_stop: # 如果释放失败
                logger.error(
                    task_id=task_id, system_name="Deviceflow", module_name="NotifyStart",
                    message=f"[notify_start]: Failed to release deviceflow of flow_id = {flow_id} when notify_start failed."
                )
            return deviceflow_pb2.OperationResponse(is_success=False)

        self._flow.update({flow_id: flow_params})
        # print(f"[NotifyStart]: self._flow = \n{self._flow}")

        # 看是否满足放到sorter的条件，如果满足，则更新状态并放到self._sorter里
        # 判断是否需要实时转发
        if Strategy.check_real_time_dispatch(strategy=self._flow[flow_id]["strategy"]):
            if flow_id not in self._sorter_params:
                # add shelfroom
                self._deviceflow._shelf_room.add_shelf(flow_id)
                # update self._sorter_params
                self._sorter_params.update({
                    flow_id: {
                        "task_id": task_id,
                        "notify_status": self._flow[flow_id]["notify_start_called"]
                    }
                })
                # update sorter
                self._flow[flow_id]["to_sort"] = True
                is_update_flow = self._deviceflow.update_flow(flow_id, task_id, self._flow[flow_id])
                if not is_update_flow:
                    logger.error(
                        task_id=task_id, system_name="Deviceflow", module_name="NotifyStart",
                        message=f"[notify_start]: Failed to update flow_params in repo when update sorter of real_time strategy."
                    )
                self._sorter.update(self._sorter_params)

                # dispatcher更新
                is_has_shelf = self._deviceflow._shelf_room.has_shelf(flow_id)
                if not is_has_shelf:
                    is_stop = self.stop(task_id=task_id, flow_id=flow_id, delete_flow_id=True)
                    if not is_stop:  # 如果停止失败
                        logger.error(
                            task_id=task_id, system_name="Deviceflow", module_name="NotifyStart",
                            message=f"[notify_start]: Failed to release deviceflow of flow_id = {flow_id} when is_has_shelf==False."
                        )
                    return deviceflow_pb2.OperationResponse(is_success=False)
                flow_dispatcher = Dispatcher(
                    task_id=task_id,
                    flow_id = self._flow[flow_id]["flow_id"],
                    shelf_room=self._deviceflow._shelf_room,
                    strategy = self._flow[flow_id]["strategy"],
                    outbound_service = self._flow[flow_id]["outbound_service"]
                )
                self._dispatcher.update({flow_id: flow_dispatcher})
            else:
                self._sorter_params[flow_id]["notify_status"] = self._flow[flow_id]["notify_start_called"]
                self._sorter.update(self._sorter_params)
        else:
            if not self._deviceflow._shelf_room.has_shelf(flow_id):
                self._deviceflow._shelf_room.add_shelf(flow_id)

            # update self._sorter_params
            self._sorter_params.update({
                flow_id: {
                    "task_id": task_id,
                    "notify_status": self._flow[flow_id]["notify_start_called"]
                }
            })
            self._flow[flow_id]["to_sort"] = True
            is_update_flow = self._deviceflow.update_flow(flow_id, task_id, self._flow[flow_id])
            if not is_update_flow:
                logger.error(
                    task_id=task_id, system_name="Deviceflow", module_name="NotifyStart",
                    message=f"[notify_start]: Failed to update flow_params in repo when update sorter of flow strategy."
                )
            self._sorter.update(self._sorter_params)

        compute_resource = request.compute_resource
        logger.info(
            task_id=task_id, system_name="Deviceflow", module_name="NotifyStart",
            message=f"[notify_start]: {compute_resource} Success."
        )
        return deviceflow_pb2.OperationResponse(is_success=True)

    def NotifyComplete(self, request, context):
        task_id = request.task_id
        flow_id = request.routing_key
        compute_resource = request.compute_resource
        if not hasattr(ComputeResource, compute_resource):
            logger.error(
                task_id=task_id, system_name="Deviceflow", module_name="NotifyComplete",
                message=f"[notify_complete]: flow_id = {flow_id}, task_id = {task_id} compute resource type error."
            )
            return deviceflow_pb2.OperationResponse(is_success=False)
        is_success, flow_params = self._deviceflow.notify_complete(self._flow, request)
        if not is_success: # 如果notify_complete失败了
            logger.error(
                task_id=task_id, system_name="Deviceflow", module_name="NotifyComplete",
                message=f"[notify_complete]: Failed to notify_complete deviceflow of flow_id = {flow_id}, task_id = {task_id}, compute_resource = {compute_resource}."
            )
            is_stop = self.stop(task_id=task_id, flow_id=flow_id, delete_flow_id=True)
            if not is_stop: # 如果停止失败
                logger.error(
                    task_id=task_id, system_name="Deviceflow", module_name="NotifyComplete",
                    message=f"[notify_complete]: Failed to release deviceflow of flow_id = {flow_id} when notify_complete failed."
                )
            return deviceflow_pb2.OperationResponse(is_success=False)
        # 判断是否需要转发给dispatcher
        is_prepare_to_dispatch = self._deviceflow.check_all_notify_complete(
            task_registry=self._registry[task_id],
            flow_params=self._flow[flow_id]
        )
        if is_prepare_to_dispatch:
            # 判断实时转发
            if Strategy.check_real_time_dispatch(strategy=self._flow[flow_id]["strategy"]):
                # 实时转发设置
                flow_dispatcher = self._dispatcher[flow_id]
                flow_dispatcher.release_dispatch()
                self._dispatcher.update({flow_id: flow_dispatcher})
            else:
                # 非实时转发
                is_has_shelf = self._deviceflow._shelf_room.has_shelf(flow_id)
                if not is_has_shelf:
                    is_stop = self.stop(task_id=task_id, flow_id=flow_id, delete_flow_id=True)
                    if not is_stop:  # 如果停止失败
                        logger.error(
                            task_id=task_id, system_name="Deviceflow", module_name="NotifyComplete",
                            message=f"[notify_complete]: Failed to release deviceflow of flow_id = {flow_id} when is_has_shelf==False."
                        )
                    return deviceflow_pb2.OperationResponse(is_success=False)
                flow_dispatcher = Dispatcher(
                    task_id=task_id,
                    flow_id = self._flow[flow_id]["flow_id"],
                    shelf_room = self._deviceflow._shelf_room,
                    strategy = self._flow[flow_id]["strategy"],
                    outbound_service = self._flow[flow_id]["outbound_service"],
                )
                flow_dispatcher.release_dispatch()
                self._dispatcher.update({flow_id: flow_dispatcher})
        logger.info(
            task_id=task_id, system_name="Deviceflow", module_name="NotifyComplete",
            message=f"[notify_complete]: {compute_resource} Success."
        )
        return deviceflow_pb2.OperationResponse(is_success=True)


    def RegisterTask(self, request, context):
        """
        example:
        self._registry = {
            "task_id": {
                "total_compute_resources": ["logical_simulation", "device_simulation"]
            }
        }
        """
        for compute_resource in request.total_compute_resources:
            if not hasattr(ComputeResource, compute_resource):
                logger.error(
                    task_id=request.task_id, system_name="Deviceflow", module_name="RegisterTask",
                    message=f"[register_task]: Failed to register task of task_id={request.task_id} compute resource error."
                )
                return deviceflow_pb2.OperationResponse(is_success=False)

        register = TaskOrientedDeviceFlowRegistry()
        success = register.register_task(
            task_id = request.task_id,
            total_compute_resources = request.total_compute_resources
        )
        if success:
            self._registry.update(register._task_registry)
            # print(f"self._registry = {self._registry}")
            logger.info(task_id=request.task_id, system_name="Deviceflow", module_name="RegisterTask",
                        message=f"[register_task] is success {success}")
        else:
            logger.error(
                task_id=request.task_id, system_name="Deviceflow", module_name="RegisterTask",
                message=f"[register_task]: Failed to register task of task_id={request.task_id}."
            )
        return deviceflow_pb2.OperationResponse(is_success=success)


    def GetTotalComputeResources(self, request, context):
        pass

    def UnRegisterTask(self, request, context):
        task_id = request.task_id

        unregister = TaskOrientedDeviceFlowRegistry()
        unregister.unregister_task(task_id = request.task_id)

        is_stop = self.stop(task_id=task_id, delete_task_id=True)
        # 可能还得停止掉task_id对应的所有Dispatcher的线程
        logger.info(task_id=task_id, system_name="Deviceflow", module_name="UnRegisterTask",
                    message=f"[unregister_task] is success {is_stop}")
        return deviceflow_pb2.OperationResponse(is_success=is_stop)

    def CheckDeviceflowDispatchFinished(self, request, context):
        task_id = request.task_id
        # 找到数据库中满足task_id的所有flow_id
        conditions = {'task_id': task_id}
        flow_ids = self._deviceflow_repo.get_values_by_conditions(item="flow_id", **conditions)
        # flow_ids 会是 [None, "xxx"]的形式
        # 检查是否已经完成了
        flow_finished_num = 0
        for flow_id in flow_ids:
            if flow_id is not None:
                if self._flow[flow_id]["isFinished"]:
                    flow_finished_num = flow_finished_num + 1
        if None in flow_ids:
            len_flow_ids = len(flow_ids) - 1
        else:
            len_flow_ids = len(flow_ids)
        if flow_finished_num == len_flow_ids:
            all_finished = True
        else:
            all_finished = False
        # 如果已经完成
        if all_finished:
            return deviceflow_pb2.OperationResponse(is_success=True)
        else:
            return deviceflow_pb2.OperationResponse(is_success=False)

    def sort(self):
        self._sorter.sort()

    def dispatch(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            while True:
                if self._dispatcher: #如果dispatcher不为空
                    for flow_id in list(self._dispatcher.keys()): #遍历self._dispatcher
                        if not self._flow.get(flow_id, {}).get("to_dispatch"):
                            self._flow[flow_id]["to_dispatch"] = True
                            is_update_flow = self._deviceflow.update_flow(
                                flow_id = flow_id,
                                task_id = self._flow[flow_id]["task_id"],
                                flow_params = self._flow[flow_id]
                            )
                            if is_update_flow:
                                flow_dispatcher = self._dispatcher[flow_id]
                                executor.submit(flow_dispatcher.dispatch)
                            else:
                                self._flow[flow_id]["to_dispatch"] = False
                        else:
                            continue
                time.sleep(2)

    def flow_release(self):
        while True:
            for flow_id in list(self._dispatcher.keys()):  # 遍历self._dispatcher
                flow_dispatcher = self._dispatcher[flow_id]
                if flow_dispatcher._is_finished: # 如果dispatch完成了
                    print(f"flow_id = {flow_id}, dispatch finished!")
                    # 先更新flow
                    self._flow[flow_id]["isFinished"] = True
                    is_update_flow = self._deviceflow.update_flow(
                        flow_id=flow_id,
                        task_id=self._flow[flow_id]["task_id"],
                        flow_params=self._flow[flow_id]
                    )
                    if not is_update_flow:
                        continue
                    # 清除相关的flow
                    is_stop = self.stop(task_id=self._flow[flow_id]["task_id"], flow_id=flow_id, delete_flow_id=True)
                    if not is_stop:
                        continue
                    print(f"flow_id = {flow_id}, stop success!")
            time.sleep(2)

    def stop(self, flow_id=None, task_id=None, delete_flow_id=False, delete_task_id=False):
        is_success = True
        if (delete_flow_id is False) and (delete_task_id is False):
            return False
        if (flow_id is None) and (task_id is None):
            return False
        # 删除task_id注册相关的信息
        if delete_task_id:
            if task_id is None:
                logger.error(
                    task_id=task_id, system_name="Deviceflow", module_name="release",
                    message=f"[release]: Failed to delete task id because task id is none."
                )
                return False
            _ = self._registry.pop(task_id, None)     # 从self._registry里删除
            is_delete_item = self._deviceflow_repo.delete_item(
                identify_name="task_id",
                identify_value=task_id,
                log_params={"task_id": task_id, "system_name": "Deviceflow", "module_name": "release"}
            )
            if not is_delete_item:  # 如果删除失败
                logger.error(
                    task_id=task_id, system_name="Deviceflow", module_name="release",
                    message=f"[release]: Failed to release deviceflow of task_id = {task_id}."
                )
                is_success = False

        # 删除flow_id的相关信息
        if delete_flow_id:
            if flow_id is None:
                logger.error(
                    task_id=task_id, system_name="Deviceflow", module_name="release",
                    message=f"[release]: Failed to delete flow id because flow id is none."
                )
                return False
            if task_id is None:
                logger.error(
                    task_id=task_id, system_name="Deviceflow", module_name="release",
                    message=f"[release]: Failed to delete flow id because task id is none."
                )
                return False
            _ = self._flow.pop(flow_id, None)                # 从self._flow里删除
            _ = self._sorter_params.pop(flow_id, None)       # 从self._sorter里删除
            _ = self._dispatcher.pop(flow_id, None)          # 从self._dispatcher里删除

            is_delete_item = self._deviceflow_repo.delete_item(
                identify_name = "flow_id",
                identify_value = flow_id,
                log_params={"task_id": task_id, "system_name": "Deviceflow", "module_name": "release"}
            )
            if not is_delete_item: #如果删除失败
                logger.error(
                    task_id=task_id, system_name="Deviceflow", module_name="release",
                    message=f"[release]: Failed to release deviceflow of task_id = {task_id}."
                )
                is_success = False

            try:
                is_remove_flow_producer = self._deviceflow._shelf_room.close_producer(flow_id)
                is_remove_flow_consumer = self._deviceflow._shelf_room.close_consumer(flow_id)
            except Exception as e:
                print(e)
                is_success = False
                pass
            is_remove_shelf_room = self._deviceflow._shelf_room.remove_shelf(flow_id)

        return is_success

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    port = get_port(DEVICEFLOW_CONFIG_PATH)
    max_workers = get_max_workers(DEVICEFLOW_CONFIG_PATH)

    inbound_pulsar_url = get_source_url(DEVICEFLOW_CONFIG_PATH)
    inbound_pulsar_topic = get_source_topic(DEVICEFLOW_CONFIG_PATH)
    shelfroom_url = get_shelfroom_url(DEVICEFLOW_CONFIG_PATH)
    shelfroom_namespace = get_shelfroom_namespace(DEVICEFLOW_CONFIG_PATH)
    print("inbound_pulsar_url", inbound_pulsar_url)
    print("inbound_pulsar_topic", inbound_pulsar_topic)

    deviceflow_grpc_service = DeviceFlowGrpcService(
        inbound_pulsar_url, inbound_pulsar_topic,
        shelfroom_url, shelfroom_namespace
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers))
    deviceflow_pb2_grpc.add_TaskOperatorOrientedDeviceFlowServicer_to_server(deviceflow_grpc_service, server)
    server.add_insecure_port("[::]:" + port)

    server.start()
    logging.info(f"Deviceflow server started, listening on {port}")
    server.wait_for_termination()