import re
import json
import logging

import pulsar
from ols.ofl_commons.infrastructure.FragmentRepo.fragment_repo import FragmentRepo, Fragment

class JsonFragmentRepo(FragmentRepo):
    def __init__(self, config: dict):
        self.client = pulsar.Client(config['service_url'])
        self.message_consumer = self.client.subscribe(
            config['pulsar_topic'],
            config['subscription'],
            consumer_type=pulsar.ConsumerType.Shared,
        )

    def get_fragment(self) -> Fragment:
        """Listen to the pulsar message queue and receive local fragment json.
        :return: class: `Fragment` object.
        """
        # Load local fragment from message queue
        try:
            msg = self.message_consumer.receive()
            self.message_consumer.acknowledge(msg)
            message = re.sub(b'[\r\n\s]+', b'', msg.data())
            local_fragment = Fragment.parse_obj(json.loads(message))
        except Exception as e:
            self.message_consumer.acknowledge(msg)
            logging.warning(f"Error message has been dropped, because {e}.")
            return None
        return local_fragment

    def __del__(self):
        self.client.close()

if __name__ == "__main__":
    import yaml
    with open('../../../manager_config.yaml') as file:
        manager_config = yaml.load(file, Loader=yaml.FullLoader)

    fragment_repo = JsonFragmentRepo(manager_config)
    for i in range(10):
        result = fragment_repo.get_fragment()
        print(f"{type(result)}, {result}")
