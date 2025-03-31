import copy
import sys
import json
import argparse

import ray
from ray.util import remove_placement_group
from ray.util.actor_pool import ActorPool
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

# 路径参数
PYTHONPATH = "/home/code/olearning-simulator/python"
PROTOPATH = "/home/code/olearning-simulator/python/ols/proto"
RAY_LOCAL_DATA_PATH = "/home/ray/tempdata"
# # starfire
# PYTHONPATH = "/home/notebook/code/personal/80378842/olearning-simulator/python"
# PROTOPATH = "/home/notebook/code/personal/80378842/olearning-simulator/python/ols/proto"
# RAY_LOCAL_DATA_PATH = "/home/notebook/data/personal/80378842/ray/tempdata"

# 设置PYTHONPATH
sys.path.append(PYTHONPATH)
sys.path.append(PROTOPATH)

# extra import
from ols.taskMgr.utils.utils_run_task import Params, Deviceflow, Actor
from ols.taskMgr.utils.utils_run_task import generate_outbound_service
# from ols.taskMgr.utils.operator_factory import OperatorFactory
from ols.taskMgr.utils.operatorflow import OperatorFlow

class RayRunner:
    def __init__(self):
        self.operatorflow = None
        self.operator_list = []
        self.params = None
        self.logger = None
        self.get_params()
        self.logger = self.params.logger

    def get_params(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--task", type=str, help="get task params in json type")
        args = parser.parse_args()
        params = json.loads(args.task)
        params.update({
            "python_path": PYTHONPATH,
            "ray_local_data_path": RAY_LOCAL_DATA_PATH
        })
        self.params = Params(params=params) #self.params是Params类的一个实例

    def initiate(self):
        # 创建含启动和停止条件的算子流
        self.operatorflow = OperatorFlow(
            task_id = self.params.task_id,
            round = self.params.operatorflow_setting.get("round", 0),
            start_params=self.params.operatorflow_setting.get("start", {}).get("logical_simulation"),
            stop_params = self.params.operatorflow_setting.get("stop", {}).get("logical_simulation"),
        )
        # 获取算子列表
        self.operator_list = self.params.operator_list

    def construct_run_params(self, operator, rayrunner_params):
        # actor主程序执行时所需要的一些公用参数
        config_params = {
            "python_path": rayrunner_params.PYTHONPATH,
            "ray_local_data_path": rayrunner_params.RAY_LOCAL_DATA_PATH,
            "manager_config": rayrunner_params.MANAGER_CONFIG,
            "deviceflow_config": rayrunner_params.DEVICEFLOW_CONFIG,
            "task_type_config": rayrunner_params.TASK_TYPE_CONFIG
        }
        # 构建每个actor的运行参数，包括与actor相关的运行参数，以及公用参数
        target_data_list = rayrunner_params.target_data_list
        resource_request_list = rayrunner_params.resource_request_list
        run_params_list = []
        for data_index, target_data in enumerate(target_data_list): #默认target_data和resource_request在devices上的顺序一致
            split_index = 0
            data_params = copy.deepcopy(target_data) #数据相关的参数
            simulation_num = sum(data_params.get("simulation_target", {}).get("nums", []))
            # print(f"data_index = {data_index}, simulation_num = {simulation_num}")
            removed_value = data_params.pop("simulation_target", None)
            simulation_target = target_data.get("simulation_target", {})
            resource_request = resource_request_list[data_index]
            devices_list = simulation_target.get("devices", [])
            for device_index, device in enumerate(devices_list):
                target_simulate_num = simulation_target.get("nums", [])[device_index]
                resource_request_num = resource_request.get("num_request", [])[device_index]
                if resource_request_num > 0: #如果真申请了资源
                    # 计算每个actor
                    num_for_actors = [target_simulate_num // resource_request_num] * resource_request_num
                    for i in range(target_simulate_num % resource_request_num):
                        num_for_actors[i] = num_for_actors[i] + 1
                    for i in range(resource_request_num):
                        split_index_list = [split_index + j for j in range(num_for_actors[i])]
                        split_index = split_index + num_for_actors[i]
                        run_params = {
                            "config": config_params,
                            "task_id": rayrunner_params.task_id,
                            "current_round": rayrunner_params.current_round,
                            "data": data_params,
                            "operator": operator,
                            "device_type": device,
                            "simulation_num": simulation_num,
                            "split_index_list": split_index_list
                        }
                        run_params_list.append(run_params)
        return run_params_list

    def get_actor_pool(self, computation_unit, run_params_list):
        # 整理computation_unit的组织形式
        device_list = computation_unit.get("devices", [])
        computation_unit_setting = computation_unit.get("setting", [])
        computation_unit_dict = {}
        for device_index, device in enumerate(device_list):
            computation_unit_dict.update({
                f"{device}": {
                    "CPU": int(computation_unit_setting[device_index].get("num_cpus", 0))
                }
            })
        # 构造actor池
        worker_config_list = []
        actors_list = []
        for run_params in run_params_list:
            device_type = run_params.get("device_type", "")
            worker_config = computation_unit_dict.get(f"{device_type}", {})
            worker_config_list.append(worker_config)
        pg = placement_group(worker_config_list, strategy="SPREAD")
        for i, run_params in enumerate(run_params_list):
            actor_setting = Actor.options(
                num_cpus = worker_config_list[i].get("CPU"),
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg,
                    placement_group_bundle_index=i)
            ).remote()
            actors_list.append(actor_setting)
        pool = ActorPool(actors_list)
        return pool, pg

    def actor_run(self, computation_unit, run_params_list):
        pool, pg = self.get_actor_pool(
            computation_unit = computation_unit,
            run_params_list = run_params_list
        )
        status_list_remote = []
        pool.map_unordered(lambda a, v: status_list_remote.append(a.run.remote(v)), run_params_list)
        status_list = ray.get(status_list_remote)
        remove_placement_group(pg)
        return status_list

    def analyze_results(self, operator, status_list, run_params_list):
        task_id = self.params.task_id
        operator_name = operator.get("name", "")
        task_repo = self.params.task_repo
        logger = self.logger

        # 构造原始的logical_result
        try:
            logical_result_string = task_repo.get_item_value(task_id=task_id, item="logical_target")
            logical_result_dict = json.loads(logical_result_string)
        except:
            logger.error(task_id=task_id, system_name="Ray Runner", module_name="run_task",
                         message=f"[analyze_results] get logical_result_dict from database failed!")
            raise Exception(f"[analyze_results]: get logical_result_dict from database failed!")

        logical_result_dict.update({
            "logical_result": logical_result_dict.get("logical_target", {})
        })
        removed_value = logical_result_dict.pop("logical_target", None)
        logical_result = logical_result_dict.get("logical_result", {})

        data_name_list = []    # e.g. ['data_0', 'data_1']
        device_type_list = []  # e.g. [['High', 'Low'], ['Middle']]
        for data_logical in logical_result:
            simulation_target = data_logical.get("simulation_target", {})
            removed_value = simulation_target.pop("nums", None)
            simulation_target.update({
                "success_num": [0]*len(simulation_target.get("devices", [])),
                "failed_num": [0]*len(simulation_target.get("devices", []))
            })
            data_logical.update({"simulation_target": simulation_target})
            data_name = data_logical.get("name", "")
            data_name_list.append(data_name)
            device_type_all = data_logical.get("simulation_target", {}).get("devices", [])
            device_type_list.append(device_type_all)

        for i, status in enumerate(status_list):
            device_type = run_params_list[i].get("device_type", "")
            data_name = status.get("data_name", "")
            if data_name in data_name_list:
                data_index = data_name_list.index(data_name)
                device_type_all = device_type_list[data_index]
                if device_type in device_type_all:
                    device_index = device_type_all.index(device_type)
                    # 更新logical_result
                    logical_result[data_index]["simulation_target"]["success_num"][device_index] \
                        += status.get("success", 0)
                    logical_result[data_index]["simulation_target"]["failed_num"][device_index] \
                        += status.get("failed", 0)

        if not task_repo.set_item_value(task_id=task_id, item="logical_round", value=(self.params.current_round + 1)):
            logger.error(task_id=task_id, system_name="Ray Runner", module_name="run_task",
                         message=f"[analyze_results] set logical_round={self.params.current_round} of task_id={task_id} failed!")
            raise Exception(f"[analyze_results] set logical_round={self.params.current_round} of task_id={task_id} failed!")
        if not task_repo.set_item_value(task_id=task_id, item="logical_operator", value=operator_name):
            logger.error(task_id=task_id, system_name="Ray Runner", module_name="run_task",
                         message=f"[run] set logical_operator={operator_name} of task_id={task_id} failed!")
            raise Exception(f"[run] set logical_operator={operator_name} of task_id={task_id} failed!")
        if not task_repo.set_item_value(task_id=task_id, item="logical_result", value=json.dumps(logical_result_dict)):
            logger.error(task_id=task_id, system_name="Ray Runner", module_name="run_task",
                         message=f"[run] set logical_result={json.dumps(logical_result_dict)} of task_id={task_id} failed!")
            raise Exception(f"[run] set logical_result={json.dumps(logical_result_dict)} of task_id={task_id} failed!")

    def run(self):
        self.initiate()
        # 算子流循环
        for round_idx in range(self.operatorflow.round):
            # print(f"round_idx = {round_idx}, self.operatorflow.round = {self.operatorflow.round}")
            ### 更新参数 ###
            self.params.current_round = round_idx
            ### 执行 ###
            # 算子流启动
            operatorflow_start_status = self.operatorflow.start()
            if not operatorflow_start_status:
                self.logger.error(task_id=self.params.task_id, system_name="Ray Runner", module_name="run_task",
                                 message=f"[run]: round {round_idx}, operatorflow start failed!")
                raise Exception(f"[run]: round {round_idx}, operatorflow start failed!")

            # 算子逐个执行
            for operator in self.operator_list: # operator is a dict
                operator_name = operator.get("name", "")
                self.logger.info(task_id=self.params.task_id, system_name="Ray Runner", module_name="run_task",
                            message=f"[run]: round {round_idx}, operator_name = {operator_name} begin!")

                # 使用Deviceflow
                use_deviceflow = operator.get("operation_behavior_controller", {}).get("use_gradient_house", False)
                operator_deviceflow = None
                if use_deviceflow:
                    # 获取output的pulsar
                    outbound_service_jsonstring = operator.get("operation_behavior_controller", {}).get("outbound_service", "")
                    outbound_service = generate_outbound_service(outbound_service_jsonstring)
                    routing_key = f"{self.params.task_id}_{operator_name}_{round_idx}"

                    # 构建实例
                    operator_deviceflow = Deviceflow(
                        use = use_deviceflow,
                        routing_key = routing_key,
                        strategy = operator.get("operation_behavior_controller", {}).get("strategy_gradient_house", ""),
                        compute_resource = "logical_simulation",
                        outbound_service = outbound_service,
                        task_id = f"{self.params.task_id}",
                        config_path = self.params.DEVICEFLOW_CONFIG,
                        logger = self.params.logger
                    )
                    # 启动梯度中间站
                    operator_deviceflow_start_status = operator_deviceflow.start()
                    if not operator_deviceflow_start_status:
                        self.logger.error(task_id=self.params.task_id, system_name="Ray Runner", module_name="run_task",
                                         message=f"[run]: round {round_idx}, deviceflow start failed!")
                        raise Exception(f"[run]: round {round_idx}, deviceflow start failed!")
                    # deviceflow参数更新
                    deviceflow_pulsar_client_info = operator_deviceflow.get_deviceflow_pulsar_client()
                    deviceflow_websocket_info = operator_deviceflow.get_deviceflow_websocket()
                    if deviceflow_pulsar_client_info is None and deviceflow_websocket_info is None:
                        raise Exception(f"[run]: round {round_idx}, deviceflow_pulsar_client_info and deviceflow_websocket_info are None")

                    if deviceflow_pulsar_client_info is not None:
                        deviceflow_available_url = deviceflow_pulsar_client_info.url
                        deviceflow_available_topic = deviceflow_pulsar_client_info.topic
                        operator["operation_behavior_controller"].update({
                            "routing_key": routing_key,
                            "compute_resource": "logical_simulation",
                            "outbound_service_pulsar_url": deviceflow_available_url,
                            "outbound_service_pulsar_topic": deviceflow_available_topic
                        })

                    if deviceflow_websocket_info is not None:
                        deviceflow_available_url = deviceflow_websocket_info.url
                        operator["operation_behavior_controller"].update({
                            "routing_key": routing_key,
                            "compute_resource": "logical_simulation",
                            "outbound_service_websocket_url": deviceflow_available_url
                        })

                # 构造运行参数
                run_params_list = self.construct_run_params(
                    operator = operator,
                    rayrunner_params = self.params
                )
                print(f"run_params_list = \n{run_params_list}\n")

                # 分配actors执行算子，并获取各Actor的结果
                status_list = self.actor_run(
                    computation_unit=self.params.computation_unit,
                    run_params_list = run_params_list
                )
                print(f"status_list = \n{status_list}\n")

                # 梯度中间站NotifyComplete
                if use_deviceflow:
                    if operator_deviceflow == None:
                        self.logger.error(task_id=self.params.task_id, system_name="Ray Runner", module_name="run_task",
                                          message=f"[run]: round {round_idx}, deviceflow stop failed when deviceflow is None")
                        raise Exception(f"[run]: round {round_idx}, deviceflow stop failed when deviceflow is None")
                    else:
                        operator_deviceflow_stop_status = operator_deviceflow.stop()
                        if not operator_deviceflow_stop_status:
                            self.logger.error(task_id=self.params.task_id, system_name="Ray Runner", module_name="run_task",
                                              message=f"[run]: round {round_idx}, deviceflow stop failed!")
                            raise Exception(f"[run]: round {round_idx}, deviceflow stop failed!")

                # 分析结果
                self.analyze_results(operator, status_list, run_params_list)  # 分析结果


            # 算子流结束判断
            operatorflow_stop_status = self.operatorflow.stop()
            if not operatorflow_stop_status:
                self.logger.error(task_id=self.params.task_id, system_name="Ray Runner", module_name="run_task",
                                 message=f"[run]: round {round_idx}, operatorflow stop failed!")
                if round_idx < self.operatorflow.round - 1:
                    raise Exception(f"[run]: round {round_idx}, operatorflow stop failed!")
                else: #如果是最后一轮，实际上已经完成了，所以可以不用等
                    break

if __name__ == '__main__':
    ray_runner = RayRunner()
    ray_runner.run()
