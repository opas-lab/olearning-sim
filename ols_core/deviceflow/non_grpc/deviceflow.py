from ols.deviceflow.non_grpc.bound_room import InboundRoom
from ols.deviceflow.non_grpc.shelf_room import ShelfRoom
import json
from typing import Dict, Any

# 数据库及日志
import os
from ols.simu_log import Logger
from ols.utils.repo_utils import SqlDataBase
FILEPATH = os.path.abspath(os.path.dirname(__file__))             # registry.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '../..'))        # ols所在文件夹的绝对路径
DEVICEFLOW_TABLE_PATH = OLSPATH + "/config/deviceflow_table.yaml" # device_table_path
LOG_PATH = OLSPATH + "/config/repo_log.yaml"                      # log日志配置的文件路径

class DeviceFlow(object):
    def __init__(self,
                 inbound_pulsar_url: str,
                 inbound_pulsar_topic: str,
                 shelfroom_url: str,
                 shelfroom_namespace: str,
                 ):
        self.sql_table = SqlDataBase(yaml_path=DEVICEFLOW_TABLE_PATH)
        self.logger = Logger()
        self.logger.set_logger(log_yaml=LOG_PATH)
        self._inbound_room = InboundRoom(inbound_pulsar_url, inbound_pulsar_topic)
        self._shelf_room = ShelfRoom(shelfroom_url, shelfroom_namespace)


    def notify_start(self, flow, request):
        # print(f"[Deviceflow, notify_start]: flow = {flow}")
        flow_params = {}

        # get params
        task_id = request.task_id
        flow_id = request.routing_key
        strategy = request.strategy
        compute_resource = request.compute_resource
        outbound_service = {}
        if request.outbound_service.pulsar_client.url != "":
            pulsar_url = request.outbound_service.pulsar_client.url
            pulsar_topic = request.outbound_service.pulsar_client.topic
            if pulsar_url != "" and pulsar_topic != "":
                outbound_service.update({
                    "pulsar_client": {
                        "url": pulsar_url,
                        "topic": pulsar_topic
                    }
                })
        if request.outbound_service.websocket.url != "":
            websocket_url = request.outbound_service.websocket.url
            if websocket_url != "":
                outbound_service.update({
                    "websocket": {
                        "url": websocket_url,
                    }
                })

        if flow_id not in flow:
            flow_params.update({
                "isFinished": False,
                "to_sort": False,
                "to_dispatch": False,
                "task_id": task_id,
                "flow_id": flow_id,
                "outbound_service": outbound_service,
                "strategy": strategy,
                "notify_start_called": {},
                "notify_complete_called": {}
            })

            # 更新数据库里的flow_id
            is_update_flow_id = self.update_flow_id(flow_id, task_id)
            if not is_update_flow_id:
                self.logger.error(task_id=task_id, system_name="Deviceflow", module_name="notify_start",
                                  message=f"[set_item_value]: Failed to set flow_id={flow_id} "
                                          f"of task_id={task_id} in repo.")
                return False, {}
        else:
            flow_params = flow[flow_id]
            # 比较值是否相同
            if task_id != flow_params["task_id"]:
                self.logger.error(task_id=task_id, system_name="Deviceflow", module_name="deviceflow",
                                  message=f"[notify_start]: task_id from logical simulation is not equal to task_id "
                                          f"from device simulation of flow_id={flow_id}")
                return False, {}
            if strategy != flow_params["strategy"]:
                self.logger.error(task_id=task_id, system_name="Deviceflow", module_name="deviceflow",
                                  message=f"[notify_start]: strategy from logical simulation is not equal to strategy "
                                          f"from device simulation of task_id={task_id}")
                return False, {}
            if request.outbound_service.pulsar_client.url != "":
                # 比较outbound_service的pulsar_client是否相同
                if flow_params["outbound_service"].get("pulsar_client", {}):
                    if flow_params["outbound_service"]["pulsar_client"] != outbound_service["pulsar_client"]:
                        self.logger.error(task_id=task_id, system_name="Deviceflow", module_name="deviceflow",
                                          message=f"[notify_start]: pulsar_client in outbound_service from logical simulation "
                                                  f"is not equal to pulsar_client in outbound_service from device simulation of task_id={task_id}")
                        return False, {}
                else:
                    if outbound_service.get("pulsar_client", {}):
                        flow_params["outbound_service"]["pulsar_client"] = outbound_service["pulsar_client"]

            if request.outbound_service.websocket.url != "":
                # 比较outbound_service的pulsar_client是否相同
                if flow_params["outbound_service"].get("websocket", {}):
                    if flow_params["outbound_service"]["websocket"] != outbound_service["websocket"]:
                        self.logger.error(task_id=task_id, system_name="Deviceflow", module_name="deviceflow",
                                          message=f"[notify_start]: websocket in outbound_service from logical simulation "
                                                  f"is not equal to websocket in outbound_service from device simulation of task_id={task_id}")
                        return False, {}
                else:
                    if outbound_service.get("websocket", {}):
                        flow_params["outbound_service"]["websocket"] = outbound_service["websocket"]

        flow_params["notify_start_called"][compute_resource] = True  # 增加notify_start_called字段

        # 写入数据库, flow
        is_update_flow = self.update_flow(flow_id, task_id, flow_params)
        if not is_update_flow:
            return False, {}
        return True, flow_params

    def notify_complete(self, flow, request):
        task_id = request.task_id
        flow_id = request.routing_key
        compute_resource = request.compute_resource

        if flow_id in flow:
            flow_params = flow[flow_id]
            # 比较值是否相同
            if task_id != flow_params["task_id"]:
                self.logger.error(task_id=task_id, system_name="Deviceflow", module_name="deviceflow",
                                  message=f"[notify_complete]: task_id from logical simulation is not equal to task_id "
                                          f"from device simulation of flow_id={flow_id}")
                return False, {}

            flow_params["notify_complete_called"][compute_resource] = True  # notify_complete_called的对应字段设值

            # 写入数据库, flow
            is_update_flow = self.update_flow(flow_id, task_id, flow_params)
            if not is_update_flow:
                return False, {}
            return True, flow_params

        else:
            return False, {}


    def check_all_notify_start(self, task_registry, flow_params):
        if len(task_registry["total_compute_resources"]) == len(flow_params["notify_start_called"]):
            if all(flow_params["notify_start_called"].values()):
                return True
        return False

    def check_all_notify_complete(self, task_registry, flow_params):
        if len(task_registry["total_compute_resources"]) == len(flow_params["notify_complete_called"]):
            if all(flow_params["notify_complete_called"].values()):
                return True
        return False

    def update_flow(self, flow_id, task_id, flow_params):
        if not self.sql_table.set_item_value(
                identify_name="flow_id", identify_value=flow_id,
                item="flow", value=json.dumps(flow_params),
                log_params={"task_id": task_id, "system_name": "Deviceflow", "module_name": "update_flow"}
        ):
            self.logger.error(task_id=task_id, system_name="Deviceflow", module_name="update_flow",
                              message=f"[set_item_value]: Failed to set flow={json.dumps(flow_params)} "
                                      f"of task_id={task_id} and flow_id={flow_id} in repo.")
            return False
        return True

    def update_flow_id(self, flow_id:str, task_id:str):
        conditions = {'flow_id': flow_id, "task_id": task_id}
        task_ids = self.sql_table.get_values_by_conditions(item="task_id", **conditions)
        if len(task_ids) == 0:
            # 新增一条数据条目
            if not self.sql_table.add_item(
                    item={
                        "task_id": [task_id],
                        "flow_id": [flow_id]
                    },
                    log_params={"task_id": task_id, "system_name": "Deviceflow", "module_name": "notify_start"}
            ):
                self.logger.error(task_id=task_id, system_name="Deviceflow", module_name="register_task",
                                  message=f"[update_flow_id]: Failed to add data item of task_id={task_id}, "
                                          f"flow_id={flow_id} in repo.")
                return False
        elif len(task_ids) == 1:
            return True
        else:
            # 存在多个task_id、flow_id的条目，有问题
            self.logger.error(task_id=task_id, system_name="Deviceflow", module_name="notify_start",
                              message=f"[update_flow_id]: There are {len(task_ids)} items corresponding to task_id={task_id}"
                                      f"and flow_id={flow_id} in repo, failed!")
            return False
        return True


