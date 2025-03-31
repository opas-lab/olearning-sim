import logging
import pulsar

from ols.deviceflow.non_grpc.utils.exception import AddShelfException
from ols.deviceflow.utils.config_parser import get_shelfroom_url, get_shelfroom_namespace


class Shelf(object):
    def __init__(self, task_id, topic_prefix):
        super(Shelf, self).__init__()
        self._task_id = task_id
        self._pulsar_url = f"{topic_prefix}/{self._task_id}"

    @property
    def task_id(self):
        return self._task_id

    @property
    def pulsar_topic(self):
        return self._pulsar_url


class ShelfRoom(object):
    def __init__(self, pulsar_url, namespace_of_shelf_room):
        super(ShelfRoom, self).__init__()

        self._pulsar_url = pulsar_url
        self._shelfroom_namespace = namespace_of_shelf_room
        self._client = pulsar.Client(self._pulsar_url)

        self.shelves = {}

        self._producers = {}
        self._consumers = {}

        self._subscription_name = "shelf_room"

    def add_shelf(self, shelf_id):
        logging.info(f"[Shelf Room][add_shelf] Adding shelf {shelf_id}...")
        if self.has_shelf(shelf_id):
            logging.warning(f"[Shelf Room][add_shelf] Shelf for shelf {shelf_id} already exists."
                            f" No new shelf {shelf_id} is added")
            return True

        try:
            shelf = Shelf(
                shelf_id, self._shelfroom_namespace
            )
            self.shelves[shelf_id] = shelf
            self.create_producer(shelf_id)
            self.create_consumer(shelf_id)
            logging.info(f"[Shelf Room][add_shelf] Add shelf {shelf_id} Successfully")
            return True
        except Exception as e:
            raise AddShelfException(shelf_id, original_exception=e) from e

    def remove_shelf(self, shelf_id):
        logging.info(f"[Shelf Room][remove_shelf] Removing shelf {shelf_id}...")
        if not self.has_shelf(shelf_id):
            return True
            # raise ValueError(f"[Shelf Room][remove_shelf]No such shelf: {shelf_id}")
        else:
            del self.shelves[shelf_id]
            return True
        # logging.info(f"[Shelf Room][remove_shelf] Remove shelf {shelf_id} successfully")

    def has_shelf(self, shelf_id):
        if shelf_id not in self.shelves:
            return False
        return True

    def get_shelf(self, shelf_id):
        logging.info(f"[Shelf Room][get_shelf] Getting shelf {shelf_id}...")
        if not self.has_shelf(shelf_id):
            logging.info(f"No shelf {shelf_id}, add shelf {shelf_id}")
            self.add_shelf(shelf_id)

        shelf = self.shelves[shelf_id]
        logging.info(f"[Shelf Room][get_shelf] Get shelf {shelf_id} successfully")
        return shelf

    def create_producer(self, shelf_id):
        shelf = self.get_shelf(shelf_id)
        producer = self._client.create_producer(
            topic=shelf.pulsar_topic,
        )
        self._producers[shelf_id] = producer
        return producer

    def get_producer(self, shelf_id):
        if shelf_id not in self._producers:
            logging.info(f"[Shelf Room][get_producer] No producer {shelf_id}, Create producer {shelf_id}")
            self.create_producer(shelf_id)

        producer = self._producers[shelf_id]
        return producer

    def create_consumer(self, shelf_id):
        shelf = self.get_shelf(shelf_id)
        consumer = self._client.subscribe(
            shelf.pulsar_topic,
            subscription_name=self._subscription_name,
        )
        self._consumers[shelf_id] = consumer
        return consumer

    def get_consumer(self, shelf_id):
        if shelf_id not in self._consumers:
            logging.info(f"[Shelf Room][get_consumer] No consumer {shelf_id}, Create consumer {shelf_id}")
            self.create_consumer(shelf_id)

        consumer = self._consumers[shelf_id]
        return consumer

    def close_consumer(self, shelf_id):
        consumer = self.get_consumer(shelf_id)
        consumer.close()
        del self._consumers[shelf_id]

    def close_producer(self, shelf_id):
        producer = self.get_producer(shelf_id)
        producer.close()
        del self._producers[shelf_id]

    def put_on_shelf(self, shelf_id, item):
        logging.debug(f"[Shelf Room][put_on_shelf] Putting item on shelf {shelf_id}...")
        logging.debug(f"[Shelf Room][put_on_shelf] item {item}")
        producer = self.get_producer(shelf_id)
        producer.send(item)
        logging.debug(f"[Shelf Room][put_on_shelf] Put item on shelf {shelf_id} successfully")
        logging.debug(f"[Shelf Room][put_on_shelf] item {item}")

    def __str__(self):
        result = ""
        for shelf_id, shelf in self.shelves.items():
            result += f"shelf_id {shelf_id}: {shelf}\n"
        return result


if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO)

    config_path_of_gradient_house = "ols/config/deviceflow_config.yaml"
    shelfroom_url = get_shelfroom_url(config_path_of_gradient_house)
    shelfroom_namespace = get_shelfroom_namespace(config_path_of_gradient_house)

    shelf_room = ShelfRoom(shelfroom_url, shelfroom_namespace)

    message = {
        "task_id": "task01",
        "content": "I am a message"
    }
    message = json.dumps(message).encode('utf-8')

    shelf_room.add_shelf("task01")
    shelf_room.put_on_shelf("task01", message)
    print(shelf_room)
    shelf_room.add_shelf("task02")
    print("add shelf", shelf_room)
    shelf_room.remove_shelf("task02")
    print("remove shelf", shelf_room)
