import yaml
import pulsar
from ols.ofl_commons.infrastructure.FragmentRepo.fragment_repo import FragmentRepo, Fragment

class ProtoFragmentRepo(FragmentRepo):
    def __init__(self, config: dict):
        self.client = pulsar.Client(config['service_url'])
        self.message_consumer = self.client.subscribe(
            config['pulsar_topic'],
            config['subscription'],
            consumer_type=pulsar.ConsumerType.Shared,
        )

    def get_fragment(self) -> Fragment:
        """Listen to the pulsar message queue and receive local fragment protobuf.
        :return: class: `Fragment` object.
        """
        # Load local fragment from message queue
        msg = self.message_consumer.receive()
        local_fragment = Fragment()
        local_fragment.deserialize(msg.data())
        try:
            self.message_consumer.acknowledge(msg)
        except Exception:
            self.message_consumer.negative_acknowledge(msg)
        return local_fragment

    def __del__(self):
        self.client.close()

if __name__ == "__main__":
    with open('../../../manager_config.yaml') as file:
        manager_config = yaml.load(file, Loader=yaml.FullLoader)

    fragment_repo = ProtoFragmentRepo(manager_config)
    for i in range(10):
        result = fragment_repo.get_fragment()
        print(f"{list(result.metrics.train_tp_fragment)}")
        print(f"{result}, \n{type(result.metrics)}, \n{type(result.metrics.train_tp_fragment)}")