from abc import ABC, abstractmethod
from typing import Dict, Any

import pulsar
import base64
import json
import websocket

class MessageProducerBase(ABC):
    def __init__(self):
        super().__init__()
        self._producer = None

    @abstractmethod
    def get_producer(self, outbound_params: Dict[str, Any]):
        """
        获取消息生产者实例。

        参数:
        outbound_params: 出站参数，用于配置消息生产者。
        """
        pass

    @abstractmethod
    def send(self, message: bytes):
        """
        发送消息。

        参数:
        message: 要发送的消息。
        """
        pass

    @abstractmethod
    def close(self):
        """
        关闭
        """
        pass


class PulsarClientProducer(MessageProducerBase):
    def __init__(self):
        super(PulsarClientProducer, self).__init__()

    def get_producer(self, outbound_params):
        target_url = outbound_params["url"]
        target_topic = outbound_params["topic"]
        client = pulsar.Client(target_url)
        self._producer = client.create_producer(target_topic)

    def send(self, message: bytes):
        self._producer.send(message)

    def close(self):
        self._producer.close()


class WebsocketProducer(MessageProducerBase):
    def __init__(self):
        super(WebsocketProducer, self).__init__()
        self._target_url = None

    def get_producer(self, outbound_params):
        self._target_url = outbound_params["url"]
        self._websocket = websocket.create_connection(self._target_url)

    def send(self, message: bytes):
        base64_message = self.base64_encode_message(message)
        message = json.dumps({"payload": base64_message})
        self._websocket.send(message)

    def base64_encode_message(self, message):
        base64_bytes = base64.b64encode(message)
        return base64_bytes.decode('utf-8')

    def close(self):
        self._websocket.close()
