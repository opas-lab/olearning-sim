import time
from typing import Dict, Any
from ols.deviceflow.non_grpc.bound_room import InboundRoom
from ols.deviceflow.non_grpc.shelf_room import ShelfRoom

from ols.deviceflow.non_grpc.utils.message import parse_routing_key, parse_message_content, parse_compute_resource

import threading
import datetime
import os
from ols.simu_log import Logger
FILEPATH = os.path.abspath(os.path.dirname(__file__))             # registry.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '../..'))        # ols所在文件夹的绝对路径
LOG_PATH = OLSPATH + "/config/repo_log.yaml"                      # log日志配置的文件路径

class Sorter(object):
    logger = Logger()
    logger.set_logger(log_yaml=LOG_PATH)

    def __init__(self,
                 inbound_room: InboundRoom,
                 shelf_room: ShelfRoom
                 ):
        # parameters
        self._inbound_room = inbound_room
        self._shelf_room = shelf_room
        self._subscription_name = "sorter"
        self._consumer = self._inbound_room.get_consumer(self._subscription_name)
        self._stop_event = threading.Event()
        self._flag_lock = threading.Lock()
        self._params = {}

    def update(self, sort_params):
        with self._flag_lock:
            self._params = sort_params

    def sort(self):
        while not self._stop_event.is_set():
            try:
                # 计算设定了timeout_millis，还是会报TimeOut Exception，坑死了
                message = self._consumer.receive(timeout_millis=1000)
            except Exception as e:
                message = None

            if message:
                if not self._should_put(message):
                    self._discard(message, self._consumer)
                    continue
                else:
                    self._put_on_shelf(message, self._consumer, self._shelf_room)
        self._consumer.close()

    def stop_sort(self):
        self._stop_event.set()

    def _should_put(self, message):
        message_value = message.value()
        try:
            flow_id = parse_routing_key(message_value)
            compute_resource = parse_compute_resource(message_value)
            notify_start_called = self._params.get(flow_id, {}).get("notify_status", {}).get(compute_resource, False)
        except Exception as e:
            # print(f"[Sorter][sort] {e}")
            return False

        if notify_start_called:
            return True
        # print("[Sorter][sort] Message '{message}' is not sent between 'notify_start' and 'notify_complete' ")
        return False

    @staticmethod
    def _discard(message, consumer):
        consumer.acknowledge(message)
        Sorter.logger.error(task_id="", system_name="Deviceflow", module_name="sorter",
                          message=f"[discard]: Message {message.value()} has been discarded")
        # print(f"Message '{message}' has been discarded.")

    @staticmethod
    def _put_on_shelf(message, consumer, shelf_room):
        message_value = message.value()
        # print(f"[Sorter][sort] Received message value from inbound room: {message_value}")
        try:
            routing_key = parse_routing_key(message_value)
            message_content = parse_message_content(message_value)
            message_content = message_content.encode('utf-8')
            shelf_room.put_on_shelf(routing_key, message_content)
        except Exception as e:
            # print(f"[Sorter][sort] {e}")
            pass

        consumer.acknowledge(message)

