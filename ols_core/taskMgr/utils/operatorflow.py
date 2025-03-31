import os
import time
import yaml
from ols.simu_log import Logger

import yaml
import json
import uuid
import asyncio
import logging
import websockets

# filepath
FILEPATH = os.path.abspath(os.path.dirname(__file__))  # operatorflow.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '../..'))  # ols所在文件夹的绝对路径
# log
logger = Logger()
logger.set_logger(log_yaml=OLSPATH + "/config/repo_log.yaml")


class OperatorFlowStrategy:
    def __init__(self, strategy):
        pass


class OperatorflowStrategyFactory:
    @staticmethod
    def create(strategy=None):
        if strategy == "waiting_for_global_aggregation":
            return WaitingForGlobalAggregation()
        if strategy == "sample_and_aggregation":
            return SampleAndAggregation()
        if strategy == "sample_dc_and_aggregation":
            return SampleDCAndAggregation()
        else:
            raise NotImplementedError


class OperatorFlow:
    def __init__(self, task_id, round, start_params, stop_params):
        self.task_id = task_id
        self.round = round
        self.start_params = start_params
        self.stop_params = stop_params
        self.current_round = 0

    def start(self):
        strategy = self.start_params.get("strategy", "")
        if strategy == "":
            return True
        elif strategy == "waiting_for_global_aggregation":
            operatorflow_strategy = OperatorflowStrategyFactory.create(strategy="waiting_for_global_aggregation")
            start_flag, current_round = operatorflow_strategy.start(
                task_id=self.task_id,
                wait_interval=self.start_params.get("wait_interval", 0),
                total_timeout=self.start_params.get("total_timeout", 0)
            )
            if start_flag == True and current_round is not None:
                self.current_round = current_round
                return True
            else:
                logger.error(task_id=self.task_id, system_name="RayRunner", module_name="OperatorFlow",
                             message=f"[start]: obtain_current_round_from_selection failed. ")
                return False
        elif strategy == "sample_and_aggregation":
            operatorflow_strategy = OperatorflowStrategyFactory.create(strategy="sample_and_aggregation")
            start_flag = operatorflow_strategy.start(
                task_id=self.task_id,
                wait_interval=self.start_params.get("wait_interval", 0),
                total_timeout=self.start_params.get("total_timeout", 0)
            )
            return start_flag
        elif strategy == "sample_dc_and_aggregation":
            operatorflow_strategy = OperatorflowStrategyFactory.create(strategy="sample_dc_and_aggregation")
            start_flag = operatorflow_strategy.start(
                task_id=self.task_id,
                wait_interval=self.start_params.get("wait_interval", 0),
                total_timeout=self.start_params.get("total_timeout", 0)
            )
            return start_flag

        else:
            logger.error(task_id=self.task_id, system_name="RayRunner", module_name="OperatorFlow",
                         message=f"[start]: The start strategy = {strategy} of operatorflow for {self.task_id} "
                                 f"is not yet implemented.")
            return False

    def stop(self):
        strategy = self.stop_params.get("strategy", "")
        if strategy == "":
            return True
        elif strategy == "waiting_for_global_aggregation":
            operatorflow_strategy = OperatorflowStrategyFactory.create(strategy="waiting_for_global_aggregation")
            stop_flag, current_round = operatorflow_strategy.stop(
                task_id=self.task_id,
                wait_interval=self.stop_params.get("wait_interval", 0),
                total_timeout=self.stop_params.get("total_timeout", 0),
                previous_round=self.current_round
            )
            if stop_flag == True and current_round is not None:
                self.current_round = current_round
                return True
            else:
                logger.error(task_id=self.task_id, system_name="RayRunner", module_name="OperatorFlow",
                             message=f"[stop]: current_round({current_round}) - previous_round({self.current_round})"
                                     f" is not equal to 1")
                return False
        elif strategy == "sample_and_aggregation":
            operatorflow_strategy = OperatorflowStrategyFactory.create(strategy="sample_and_aggregation")
            stop_flag = operatorflow_strategy.stop(
                task_id=self.task_id,
                wait_interval=self.stop_params.get("wait_interval", 0),
                total_timeout=self.stop_params.get("total_timeout", 0),
                previous_round=self.current_round
            )
            return stop_flag
        elif strategy == "sample_dc_and_aggregation":
            operatorflow_strategy = OperatorflowStrategyFactory.create(strategy="sample_dc_and_aggregation")
            stop_flag = operatorflow_strategy.stop(
                task_id=self.task_id,
                wait_interval=self.stop_params.get("wait_interval", 0),
                total_timeout=self.stop_params.get("total_timeout", 0),
                previous_round=self.current_round
            )
            return stop_flag


        else:
            logger.error(task_id=self.task_id, system_name="RayRunner", module_name="OperatorFlow",
                         message=f"[stop]: The start strategy = {strategy} of operatorflow for {self.task_id} "
                                 f"is not yet implemented.")
            return False


class WaitingForGlobalAggregation:
    def __init__(self):
        self.selection_config_file = OLSPATH + "/config/selection_config.yaml"

    def start(self, task_id, wait_interval, total_timeout):
        start_time = time.time()
        while time.time() - start_time <= total_timeout:
            current_round = self.obtain_current_round_from_selection(task_id)
            if current_round is not None:
                return True, current_round
            time.sleep(wait_interval)
        return False, None

    def stop(self, task_id, wait_interval, total_timeout, previous_round):
        start_time = time.time()
        while time.time() - start_time <= total_timeout:
            current_round = self.obtain_current_round_from_selection(task_id)
            if current_round is not None:
                if current_round - previous_round == 1:
                    return True, current_round
            time.sleep(wait_interval)
        return False, None

    class WebSocketClient:
        def __init__(self, url):
            self.url = url
            self.websocket_connection = None

        async def connect(self):
            self.websocket_connection = await websockets.connect(self.url)

        async def send_message(self, message):
            if self.websocket_connection:
                await self.websocket_connection.send(message)

        async def receive_message(self, message_list, timeout=10):
            if self.websocket_connection:
                try:
                    # 定义一个新的协程函数来接收消息
                    async def recv_with_timeout():
                        return await self.websocket_connection.recv()

                    # message = await self.websocket_connection.recv()
                    message = await asyncio.wait_for(recv_with_timeout(), timeout)
                    message_list.append(message)
                    await self.websocket_connection.close()
                except websockets.ConnectionClosed:
                    logging.error("WebSocket connection closed.")
                except Exception as e:
                    logging.error(f"An error occurred while receiving message.")

    class SelectionService:
        def __init__(self, selection_config):
            with open(selection_config, 'r') as file:
                config = yaml.load(file, Loader=yaml.FullLoader)
                self.distributor_url = config['service_url']

        async def ws_selection(self, task_id):
            client_id = str(uuid.uuid4())  # 生成一个新的UUID作为client_id
            message_id = str(uuid.uuid4())  # 生成一个新的UUID作为message_id
            msg = self.construct_msg(task_id, client_id, message_id)
            message_list = []
            client = WaitingForGlobalAggregation.WebSocketClient(self.distributor_url)
            await client.connect()
            await client.send_message(msg)
            await client.receive_message(message_list)
            return message_list[0]  # 返回接收到的第一条消息

        def construct_msg(self, task_id, client_id, message_id, message_type="REGISTER_MESSAGE"):
            msg_json = {
                "header": {
                    "messageId": message_id,
                    "messageType": message_type,
                    "clientId": client_id
                },
                "data": {
                    "message_id": message_id,
                    "client_id": client_id,
                    "task_id": task_id
                }
            }
            msg = json.dumps(msg_json, default=str)
            return msg

    # 基于websocket的筛选函数
    async def selection(self, task_id, selection_config):
        # 创建SelectionService实例
        service = WaitingForGlobalAggregation.SelectionService(selection_config)
        # 执行ws_selection并获取返回值
        message = await service.ws_selection(task_id)
        return message

    # TODO
    def obtain_current_round_from_selection(self, task_id):
        try:
            message = asyncio.run(
                self.selection(task_id=task_id, selection_config=self.selection_config_file)
            )
            message_json = json.loads(message)
            current_round = message_json.get("data", {}).get("round_idx", None)
        except:
            current_round = None
        return current_round


class SampleAndAggregation:
    def __init__(self):
        pass

    def start(self, task_id, wait_interval, total_timeout):
        import random
        import shutil

        sample_amount = 7000

        # source_dir = "/home/notebook/data/group/privacy/game_center/bucket_20240301/bar_80"
        source_dir = "/home/notebook/data/group/privacy/research/torch_ctr_game/data/selected_data"
        target_dir = "/home/notebook/data/group/privacy/research/torch_ctr_game/data/temp_data"
        if os.path.exists(target_dir):
            shutil.rmtree(target_dir)

        os.makedirs(target_dir, exist_ok=True)
        source_file_list = os.listdir(source_dir)
        target_file_list = random.sample(source_file_list, sample_amount)

        for target_file in target_file_list:
            src = f"{source_dir}/{target_file}"
            dst = f"{target_dir}/{target_file}"
            os.symlink(src, dst)

        return True

    def stop(self, task_id, wait_interval, total_timeout, previous_round):
        # 生成一个完成的标志文件
        file_dir = f"/home/notebook/data/group/privacy/research/torch_ctr_game/ray/tempdata/{task_id}"
        if not os.path.exists(file_dir):
            os.makedirs(file_dir, exist_ok=True, mode=0o777)

        finished_file = f"{file_dir}/simulation_finished.txt"
        aggregation_finished_file = f"{file_dir}/aggregation_finished.txt"

        if os.path.exists(finished_file):
            os.remove(finished_file)
        if os.path.exists(aggregation_finished_file):
            os.remove(aggregation_finished_file)

        with open(finished_file, "w") as txt_file:
            pass

        start_time = time.time()
        while time.time() - start_time <= total_timeout:
            if os.path.exists(aggregation_finished_file):
                os.remove(aggregation_finished_file)
                return True
            else:
                time.sleep(wait_interval)
        return False


class SampleDCAndAggregation:
    def __init__(self):
        pass

    def start(self, task_id, wait_interval, total_timeout):
        import random
        import shutil

        data_folder_num = 500  # bundles数

        data_files_num = 10  # 每个bundle里的文件数

        source_dir = "/home/notebook/data/group/privacy/research/torch_ctr_game/data/selected_data"
        target_folder = "/home/notebook/data/group/privacy/research/torch_ctr_game/data/temp_data_feddc"

        if os.path.exists(target_folder):
            shutil.rmtree(target_folder)

        os.makedirs(target_folder, exist_ok=True)

        source_file_list = os.listdir(source_dir)

        for folder_ind in range(data_folder_num):
            target_dir = f"{target_folder}/data{folder_ind}"
            os.makedirs(target_dir, exist_ok=True)
            target_file_list = random.sample(source_file_list, data_files_num)

            for target_file in target_file_list:
                src = f"{source_dir}/{target_file}"
                dst = f"{target_dir}/{target_file}"
                os.symlink(src, dst)

        return True

    def stop(self, task_id, wait_interval, total_timeout, previous_round):
        # 生成一个完成的标志文件
        file_dir = f"/home/notebook/data/group/privacy/research/torch_ctr_game/ray/tempdata/{task_id}"
        if not os.path.exists(file_dir):
            os.makedirs(file_dir, exist_ok=True, mode=0o777)

        finished_file = f"{file_dir}/simulation_finished.txt"
        aggregation_finished_file = f"{file_dir}/aggregation_finished.txt"

        if os.path.exists(finished_file):
            os.remove(finished_file)
        if os.path.exists(aggregation_finished_file):
            os.remove(aggregation_finished_file)

        with open(finished_file, "w") as txt_file:
            pass

        start_time = time.time()
        while time.time() - start_time <= total_timeout:
            if os.path.exists(aggregation_finished_file):
                os.remove(aggregation_finished_file)
                return True
            else:
                time.sleep(wait_interval)
        return False