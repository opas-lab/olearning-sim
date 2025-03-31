import logging
from typing import Optional

import pulsar
import _pulsar

from ols.deviceflow.non_grpc.utils.observer import Subject


class BoundRoom(object):
    def __init__(self, pulsar_url: str, pulsar_topic: str):
        super(BoundRoom, self).__init__()
        self._pulsar_url = pulsar_url
        self._pulsar_topic = pulsar_topic

    @property
    def pulsar_url(self):
        return self._pulsar_url

    @property
    def pulsar_topic(self):
        return self._pulsar_topic

    def show_info(self):
        logging.info(f"Pulsar URL: {self.pulsar_url}")
        logging.info(f"Topic URL: {self.pulsar_topic}")


class InboundRoom(BoundRoom, Subject):
    def __init__(self, pulsar_url: str, pulsar_topic: str):
        BoundRoom.__init__(self, pulsar_url, pulsar_topic)
        Subject.__init__(self)
        self._status = None  # opening or closed
        self._client = pulsar.Client(self.pulsar_url)

    def get_consumer(self, subscription_name):
        try:
            print("pulsar_topic", self.pulsar_topic)
            consumer = self._client.subscribe(
                self.pulsar_topic,
                subscription_name=subscription_name,
                consumer_type=pulsar.ConsumerType.Shared,
            )
            return consumer
        except Exception as e:
            logging.error(f"[Inbound Room][get_consumer] {e}")

    def get_availabl_source_url(self, flow_id: str) -> Optional[str]:
        # TODO: Implement the logic for retrieving an available source URL
        # This method should return a string that represents an available source URL,
        # or `None` if no URL is available.

        # This is a EXPEDIENT
        available_source_url = self.pulsar_url
        return available_source_url

    def get_available_source_topic(self, flow_id: str) -> Optional[str]:
        # TODO: Implement the logic for retrieving an available source pulsar_topic
        # This method should return a string that represents an available source pulsar_topic,
        # or `None` if no pulsar_topic is available.

        # This is a EXPEDIENT
        available_source_topic = self.pulsar_topic
        return available_source_topic


class OutboundRoom(BoundRoom):
    def __init__(self, pulsar_url: str, topic_url: str):
        super().__init__(pulsar_url, topic_url)
        self._client = pulsar.Client(self.pulsar_url)

    def get_producer(self):
        producer = self._client.create_producer(
            self.pulsar_topic,
        )
        return producer

    def close_producer(self):
        producer = self.get_producer()
        producer.close()


if __name__ == "__main__":
    import pulsar
    from ols.deviceflow.utils.config_parser \
        import get_source_topic, get_source_url, \
        get_pulsar_url_of_outbound_room, get_topic_url_of_outbound_room, get_subscription_of_outbound_room

    config_path_of_gradient_house = "ols/config/deviceflow_config.yaml"
    pulsar_url_of_inbound_room = get_source_url(config_path_of_gradient_house)
    topic_url_of_inbound_room = get_source_topic(config_path_of_gradient_house)

    # outbound room
    config_path_of_outbound_room = "ols/config/manager_config.yaml"
    pulsar_url_of_outbound_room = get_pulsar_url_of_outbound_room(config_path_of_outbound_room)
    topic_url_of_outbound_room = get_topic_url_of_outbound_room(config_path_of_outbound_room)
    subscription_of_outbound_room = get_subscription_of_outbound_room(config_path_of_outbound_room)

    logging.basicConfig(level=logging.INFO)

    inbound_room = InboundRoom(
        pulsar_url_of_inbound_room, topic_url_of_inbound_room
    )
    inbound_room.show_info()
    inbound_room.get_consumer("sorter1")
    inbound_room.get_consumer("sorter2")
    inbound_room.get_consumer("")

    outbound_room = OutboundRoom(
        pulsar_url_of_outbound_room, topic_url_of_outbound_room
    )
    outbound_room.show_info()
