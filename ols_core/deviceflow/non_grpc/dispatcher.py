import time
from typing import Dict, Any
from ols.deviceflow.non_grpc.shelf_room import ShelfRoom
import threading
import os
from ols.deviceflow.non_grpc.message_producer import PulsarClientProducer, WebsocketProducer
from ols.deviceflow.non_grpc.strategy import Strategy
import _pulsar

import concurrent.futures
import random
from ols.simu_log import Logger

import sqlalchemy
from sqlalchemy import MetaData, Table, insert
import traceback2 as traceback
import json

TIMEOUT_MILLIS = 1000
FILEPATH = os.path.abspath(os.path.dirname(__file__))             # dispatcher.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '../..'))        # ols所在文件夹的绝对路径
LOG_PATH = OLSPATH + "/config/repo_log.yaml"                      # log日志配置的文件路径
# log
logger = Logger()
logger.set_logger(log_yaml=LOG_PATH)

class Dispatcher(object):
    def __init__(self,
                 task_id: str,
                 flow_id: str,
                 shelf_room: ShelfRoom,
                 strategy: str,
                 outbound_service: Dict[str, Dict[str, Any]],
                 is_finished = False
                 ):
        self._task_id = task_id
        self._flow_id = flow_id
        self._shelf_room = shelf_room
        self._strategy = strategy
        self._outbound_service = outbound_service
        self._is_finished = is_finished
        self._consumer = self._shelf_room.get_consumer(self._flow_id)
        self._producer = None
        self._release_event = threading.Event()
        self._stop_event = threading.Event()

    def dispatch(self):
        # 发送完成之后，怎么确定发送完成了? -> 收不到信息，且此时notify_complete均为True
        self._producer = self.get_producer(self._outbound_service)
        is_real_time_dispatch = Strategy.check_real_time_dispatch(self._strategy)
        if is_real_time_dispatch:
            self.dispatch_real_time_with_strategy(
                consumer = self._consumer,
                producer = self._producer,
                strategy = self._strategy
            )
        else:
            self.dispatch_flow_with_strategy(
                consumer=self._consumer,
                producer=self._producer,
                strategy=self._strategy
            )
        print(f"dispatch finished!")

    def get_producer(self, outbound_service):
        if "pulsar_client" in outbound_service:
            outbound_params = outbound_service["pulsar_client"]
            producer = PulsarClientProducer()
            producer.get_producer(outbound_params)
            return producer
        elif "websocket" in outbound_service:
            outbound_params = outbound_service["websocket"]
            producer = WebsocketProducer()
            producer.get_producer(outbound_params)
            return producer
        return None

    def stop_dispatch(self):
        self._stop_event.set()

    def release_dispatch(self):
        self._release_event.set()

    def dispatch_real_time_with_strategy(self, consumer, producer, strategy):
        dispatch_amount, drop_probability = Strategy.real_time_strategy_analysis(strategy)
        logger.info(
            task_id=self._task_id, system_name="Deviceflow", module_name="Dispatcher",
            message=f"[dispatch_real_time_with_strategy]: dispatch amount: {dispatch_amount} "
                    f"drop probability:{drop_probability}."
        )
        if len(dispatch_amount) == 1:
            # TODO: 或许可以尝试获取shelf_room底下的消息总量？ -> 非admin好像无法获得相关的信息量
            send_amount = dispatch_amount[0]
            message_list = [] #还是得暂存一下，否则需要实时获取shelf_room内的消息总量做判断
            while not self._stop_event.is_set():
                try:
                    received_msg = consumer.receive(timeout_millis=TIMEOUT_MILLIS)
                    message = received_msg.value()
                    message_list.append(message)
                    consumer.acknowledge(received_msg)
                    if len(message_list) == send_amount:
                        for message in message_list:
                            if drop_probability == 0:
                                producer.send(message)
                            elif drop_probability > 0 and drop_probability < 1:
                                if random.random() > drop_probability:
                                    producer.send(message)
                            else:
                                continue
                        message_list = []
                except _pulsar.Timeout as error:
                    if self._release_event.is_set():
                        # 把剩余的全部转发掉
                        for message in message_list:
                            if drop_probability == 0:
                                producer.send(message)
                            elif drop_probability > 0 and drop_probability < 1:
                                if random.random() > drop_probability:
                                    producer.send(message)
                            else:
                                continue
                        # 设置dispatcher的_is_finished变量，使得主函数中能需要释放掉相关的数据库条目、全局变量
                        self._is_finished = True
                        self.stop_dispatch()
                        break
                    continue
                except Exception as e:
                    break
        elif len(dispatch_amount) > 1:
            # TODO: 循环发
            while not self._stop_event.is_set():
                for send_amount in dispatch_amount:
                    message_list = []
                    while not self._stop_event.is_set():
                        try:
                            received_msg = consumer.receive(timeout_millis=TIMEOUT_MILLIS)
                            message = received_msg.value()
                            message_list.append(message)
                            consumer.acknowledge(received_msg)
                            if len(message_list) == send_amount:
                                for message in message_list:
                                    if drop_probability == 0:
                                        producer.send(message)
                                    elif drop_probability > 0 and drop_probability < 1:
                                        if random.random() > drop_probability:
                                            producer.send(message)
                                    else:
                                        continue
                                message_list = []
                                # 这里需要跳出while循环, 进入for循环
                                break
                        except _pulsar.Timeout as error:
                            if self._release_event.is_set():
                                # 把剩余的全部转发掉
                                for message in message_list:
                                    if drop_probability == 0:
                                        producer.send(message)
                                    elif drop_probability > 0 and drop_probability < 1:
                                        if random.random() > drop_probability:
                                            producer.send(message)
                                    else:
                                        continue
                                # 设置dispatcher的_is_finished变量，使得主函数中能需要释放掉相关的数据库条目、全局变量
                                self._is_finished = True
                                self.stop_dispatch()
                                break
                            continue
                        except Exception as e:
                            break
        else:
            pass

    # TODO: 等完成定制化策略后，可以根据dispatch_timing和dispatch_amount发送消息
    def dispatch_flow_with_strategy(self, consumer, producer, strategy):
        dispatch_timing, dispatch_amount, drop_simulation_list = Strategy.flow_strategy_analysis(
            strategy = strategy,
            flow_id = self._flow_id
        )

        # 临时写表
        try:
            self.temp_write_table(dispatch_timing, dispatch_amount, strategy)
        except (KeyError, Exception):
            print(f"[temp_write_table]: execute failed, because of \n{traceback.format_exc()}.\n")

        logger.info(
            task_id=self._task_id, system_name="Deviceflow", module_name="Dispatcher",
            message=f"[dispatch_flow_with_strategy]: dispatch timing: {dispatch_timing} "
                    f"dispatch amount:{dispatch_amount} drop simulation list:{drop_simulation_list}."
        )
        print(f"[dispatch_flow_with_strategy]: dispatch_timing = {dispatch_timing}")
        print(f"[dispatch_flow_with_strategy]: dispatch_amount = {dispatch_amount}")
        print(f"[dispatch_flow_with_strategy]: drop_simulation_list = {drop_simulation_list}")

        if len(dispatch_timing) != len(dispatch_amount):
            # "length of dispatch_timing should be equal to dispatch_amount"
            self.clean_remain_message(consumer=consumer)
            return

        if len(dispatch_timing) == 0:
            # 不执行了，直接退出
            self.clean_remain_message(consumer=consumer)
            return

        # 先停一段时间，然后再发送定量的消息
        first_delay_time = dispatch_timing[0]
        if first_delay_time > 0:
            time.sleep(first_delay_time)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for idx in range(len(dispatch_timing)):
                send_amount = dispatch_amount[idx]
                try:
                    drop_list = drop_simulation_list[idx]
                except:
                    drop_list = []
                executor.submit(self.send_with_time_and_amount, consumer, producer, send_amount, drop_list)
                if idx < len(dispatch_timing) - 1:
                    time.sleep(dispatch_timing[idx+1])

        # 消费掉剩下的消息
        self.clean_remain_message(consumer=consumer)

    def send_with_time_and_amount(self, consumer, producer, send_amount, drop_list):
        drop_operation = False
        if len(drop_list) > 0:
            drop_operation = True
        for i in range(send_amount):
            if self._stop_event.is_set():
                break
            try:
                received_msg = consumer.receive(timeout_millis=TIMEOUT_MILLIS)
                message = received_msg.value()
                if drop_operation:
                    if i in drop_list:
                        consumer.acknowledge(received_msg)
                        continue
                producer.send(message)
                consumer.acknowledge(received_msg)
            except _pulsar.Timeout:
                break
            except Exception as e:
                break

    def clean_remain_message(self, consumer):
        while not self._stop_event.is_set():
            try:
                received_msg = consumer.receive(timeout_millis=TIMEOUT_MILLIS)
                consumer.acknowledge(received_msg)
            except:
                if self._release_event.is_set():
                    self._is_finished = True
                break

    def temp_write_table(self, dispatch_timing, dispatch_amount, strategy):
        '''
        临时写入operation_amount_table, accumulated_amount_table表，用于对外展示梯度中间站曲线。
            实时累计转发不展示；
            规则转发，只按照相对时间发送量的形式展示（因为表里的task_time类型为big_int，且有一定顺序）
                绝对时间则转为相对时间的形式来展示。
        '''
        # 确定是否使用规则转发，否则直接return
        strategy = json.loads(strategy)
        flow_dispatch = strategy.get("flow_dispatch", {})
        is_flow_strategy_dispatch = flow_dispatch.get("use_strategy", False)
        if not is_flow_strategy_dispatch:
            return

        try:
            # sql_engine = sqlalchemy.create_engine(
            #     f"mysql+pymysql://root:privacy123456@10.114.63.132:3306/virtually_phone_DB"
            # )
            sql_engine = sqlalchemy.create_engine(
                    f"mysql+pymysql://root:privacy123456@10.52.52.96:3306/virtually_phone_DB"
                )
            task_id = self._task_id
            # 确认time_type，如果不符合条件就直接return，不作图不执行
            is_specific_timing = flow_dispatch.get("specific_timing", {}).get("use", False)
            is_specific_interval = flow_dispatch.get("specific_interval", {}).get("use", False)
            if is_specific_timing:
                time_type = flow_dispatch.get("specific_timing", {}).get("time_type", "")
            elif is_specific_interval:
                time_type = flow_dispatch.get("specific_interval", {}).get("time_type", "")
            else:
                return
            if time_type != "relative" and time_type != "absolute":
                return

            ## operation_amount_table
            sql_table = Table("operation_amount_table", MetaData(), autoload=True, autoload_with=sql_engine)
            # 删除表中task_id相同的数据条目
            delete_stmt = sql_table.delete().where(sql_table.c.task_id == task_id)
            sql_engine.execute(delete_stmt)
            # 写入
            # 需注意：
            # 1. dispatch_timing是前者相对于后者的时间，比如[39, 1, 1, ...]，在写表时需要变成累计[39, 40, 41, ...]
            # 2. relative的话，dispatch_timing的第一个值保留，往前增加一个0；absolute的话则第一个值直接变为0
            accu_dispatch_timings = []
            if time_type == "relative":
                # 如果第一个值不为0，则往前增加一个0
                if dispatch_timing[0] > 0:
                    item = {"task_id": [task_id], "task_time": [0], "amount": [0]}
                    insert_stmt = insert(sql_table).values(**item)
                    sql_engine.execute(insert_stmt)
                accu_timing = 0
                for i in range(len(dispatch_timing)):
                    task_time = accu_timing + dispatch_timing[i]
                    item = {"task_id": [task_id], "task_time": [task_time], "amount": [dispatch_amount[i]]}
                    insert_stmt = insert(sql_table).values(**item)
                    sql_engine.execute(insert_stmt)
                    accu_timing = task_time
                    accu_dispatch_timings.append(accu_timing)
            elif time_type == "absolute":
                accu_timing = 0
                for i in range(len(dispatch_timing)):
                    if i == 0:
                        item = {"task_id": [task_id], "task_time": [0], "amount": [dispatch_amount[i]]}
                        insert_stmt = insert(sql_table).values(**item)
                        sql_engine.execute(insert_stmt)
                        accu_dispatch_timings.append(0)
                    else:
                        task_time = accu_timing + dispatch_timing[i]
                        item = {"task_id": [task_id], "task_time": [task_time], "amount": [dispatch_amount[i]]}
                        insert_stmt = insert(sql_table).values(**item)
                        sql_engine.execute(insert_stmt)
                        accu_timing = task_time
                        accu_dispatch_timings.append(accu_timing)
            else:
                return

            ## accumulated_amount_table
            upper_bound = 750
            # 删除表中task_id相同的数据条目
            sql_table = Table("accumulated_amount_table", MetaData(), autoload=True, autoload_with=sql_engine)
            delete_stmt = sql_table.delete().where(sql_table.c.task_id == task_id)
            sql_engine.execute(delete_stmt)
            # accu_dispatch_timings: [39, 40, 41, ...]
            # dispatch_amount, 发送量, [2000, 3000, 4000, ...]
            accu_timing = [0]
            accu_amount = [0]
            pre_amount = 0

            accu_dispatch_timings = [int(element) for element in accu_dispatch_timings]
            for i in range(len(accu_dispatch_timings)):
                tmp_timing, tmp_amount = [], []
                if i == 0:
                    if accu_dispatch_timings[i] > 0:
                        tmp_timing = [k for k in range(1, accu_dispatch_timings[i])]
                        tmp_amount = [0] * len(tmp_timing)
                        accu_timing.extend(tmp_timing)
                        accu_amount.extend(tmp_amount)

                if i <= len(accu_dispatch_timings) - 2:
                    if i == 0 and accu_dispatch_timings[i] == 0:
                        start_indx = 1
                    else:
                        start_indx = accu_dispatch_timings[i]
                    end_indx = accu_dispatch_timings[i + 1]

                    tmp_timing = [k for k in range(start_indx, end_indx)]
                    tmp_amount = [dispatch_amount[i] + pre_amount] * len(tmp_timing)
                    for j in range(len(tmp_amount)):
                        if tmp_amount[j] > pre_amount + upper_bound * (j + 1):
                            tmp_amount[j] = pre_amount + upper_bound * (j + 1)

                elif i == len(accu_dispatch_timings) - 1:
                    final_timing_start = accu_dispatch_timings[i]
                    final_timing_end = max(
                        accu_dispatch_timings[i] + 2,
                        accu_dispatch_timings[i] + (accu_dispatch_timings[i] - accu_dispatch_timings[i - 1]) // 2)
                    tmp_timing = [k for k in range(
                            final_timing_start,
                            final_timing_end)]
                    tmp_amount = [dispatch_amount[i] + pre_amount] * len(tmp_timing)
                    for j in range(len(tmp_amount)):
                        if tmp_amount[j] > pre_amount + upper_bound * (j + 1):
                            tmp_amount[j] = pre_amount + upper_bound * (j + 1)

                pre_amount = pre_amount + dispatch_amount[i]

                if tmp_timing and tmp_amount:
                    accu_timing.extend(tmp_timing)
                    accu_amount.extend(tmp_amount)

            for i in range(len(accu_timing)):
                item = {
                    "task_id": [task_id],
                    "task_time": [accu_timing[i]],
                    "amount": [accu_amount[i]]
                }
                insert_stmt = insert(sql_table).values(**item)
                sql_engine.execute(insert_stmt)

        except (KeyError, Exception):
            print(f"Writing table failed, because of \n{traceback.format_exc()}.\n")
            return