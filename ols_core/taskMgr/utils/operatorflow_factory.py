from abc import ABC, abstractmethod
from typing import List, Dict, Any

class OperatorFlow(ABC):
    @abstractmethod
    def start(self):
        raise NotImplementedError("start method must be implemented by the concrete subclass.")

    @abstractmethod
    def stop(self):
        raise NotImplementedError("stop method must be implemented by the concrete subclass.")

class DefaultOperatorFlow(OperatorFlow):
    def start(self):
        return True

    def stop(self):
        return True

class Selection_Training(OperatorFlow):
    def __init__(self, params):
        self.current_round = 0
        self.task_id = params.task_id
        self.selection_config = params.SELECTION_CONFIG

    def obtain_current_round_from_selection(self):
        import yaml, json, pulsar, uuid
        with open(self.selection_config, 'r') as file:
            selection_config = yaml.load(file, Loader=yaml.FullLoader)
        pulsar_client = pulsar.Client(selection_config['service_url'])
        pulsar_producer = pulsar_client.create_producer(selection_config['topic'][0])
        pulsar_producer.flush()
        clientId = str(uuid.uuid4())
        messageId = str(uuid.uuid4())
        fragment = {
            "header": {"clientId": clientId, "messageId": messageId, "messageType": "to_register"},
            "data": {"task_id": self.task_id, "clientId": clientId, "messageId": messageId}
        }
        fragment = json.dumps(fragment).encode('utf-8')
        try:
            pulsar_producer.send(fragment)
            consumer = pulsar_client.subscribe(selection_config['topic'][1], "simulation-test" + clientId,
                                               consumer_type=pulsar.ConsumerType.Shared)
            msg = consumer.receive()
            received_fragment = json.loads(msg.data())
            received_clientId = received_fragment.get("data", {}).get("client_id", "")
            if clientId == received_clientId:
                # 判断筛选服务请求获得的信息是否正常
                accept_status = received_fragment.get("data", {}).get("accept_status", "0")  # "1"为通过，"0"为拒绝等异常
                if accept_status != "1":  # 如果为异常状态, 包括task状态为FINISHED
                    consumer.negative_acknowledge(msg)
                    pulsar_client.close()
                    return None
                try:
                    consumer.acknowledge(msg)
                    current_round = received_fragment.get("data", {}).get("round_idx", 0)
                    pulsar_client.close()
                    return current_round
                except Exception as e:
                    consumer.negative_acknowledge(msg)
                    pulsar_client.close()
                    return None
            else:
                consumer.negative_acknowledge(msg)
                pulsar_client.close()
                return None
        except Exception as e:
            pulsar_client.close()
            return None

    def start(self):
        import time
        time_gap = 10
        time_total = 60
        start_time = time.time()
        while time.time() - start_time <= time_total:
            current_round = self.obtain_current_round_from_selection()
            if current_round is not None:
                self.current_round = current_round
                return True
            time.sleep(time_gap)
        return False

    def stop(self):
        import time
        time_gap = 10
        time_total = 60
        start_time = time.time()
        while time.time() - start_time <= time_total:
            current_round = self.obtain_current_round_from_selection()
            if current_round is not None:
                if current_round - self.current_round == 1:
                    return True
            time.sleep(time_gap)
        return False

class Training(Selection_Training):
    def start(self):
        return True

    def stop(self):
        import time
        time_gap = 10
        time_total = 60
        start_time = time.time()
        while time.time() - start_time <= time_total:
            current_round = self.obtain_current_round_from_selection()
            if current_round is not None:
                if current_round - self.current_round == 1:
                    self.current_round = self.current_round + 1
                    return True
            time.sleep(time_gap)
        return False

class OperatorFlowFactory:
    @staticmethod
    def create_operator_flow(operatorflow_name=None, params=None):
        if operatorflow_name == "selection->training":
            return Selection_Training(params)
        elif operatorflow_name == "training":
            return Training(params)
        else:
            return DefaultOperatorFlow()