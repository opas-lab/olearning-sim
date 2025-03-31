import logging
import pulsar
import _pulsar

from ols.gradient_house.non_microservice.utils.observer import Subject


class BoundRoom(object):
    def __init__(self, pulsar_url: str, topic_url: str):
        super(BoundRoom, self).__init__()
        self._pulsar_url = pulsar_url
        self._topic_url = topic_url

    @property
    def pulsar_url(self):
        return self._pulsar_url

    @property
    def topic_url(self):
        return self._topic_url

    def show_info(self):
        logging.info(f"Pulsar URL: {self.pulsar_url}")
        logging.info(f"Topic URL: {self.topic_url}")


class InboundRoom(BoundRoom, Subject):
    def __init__(self, pulsar_url: str, topic_url: str):
        BoundRoom.__init__(self, pulsar_url, topic_url)
        Subject.__init__(self)
        self._status = None  # opening or closed

        self._client = pulsar.Client(self.pulsar_url)

    def get_consumer(self, subscription_name):
        try:
            consumer = self._client.subscribe(
                self.topic_url,
                subscription_name=subscription_name,
                consumer_type=pulsar.ConsumerType.Shared,
            )
            return consumer
        except _pulsar.InvalidConfiguration:
            logging.info("[Inbound Room] Invalid subscription_name")


class OutboundRoom(BoundRoom):
    def __init__(self, pulsar_url: str, topic_url: str):
        super().__init__(pulsar_url, topic_url)
        self._client = pulsar.Client(self.pulsar_url)

    def get_producer(self):
        producer = self._client.create_producer(
            self.topic_url,
        )
        return producer


if __name__ == "__main__":
    import pulsar

    from ols.test.gradient_house.config.gradient_house_config \
        import outbound_pulsar_url, outbound_topic_url, inbound_pulsar_url, inbound_topic_url

    logging.basicConfig(level=logging.INFO)

    inbound_room = InboundRoom(
        inbound_pulsar_url, inbound_topic_url
    )
    inbound_room.show_info()
    inbound_room.get_consumer("sorter1")
    inbound_room.get_consumer("sorter2")
    inbound_room.get_consumer("")

    outbound_room = OutboundRoom(
        outbound_pulsar_url, outbound_topic_url
    )
    outbound_room.show_info()
