import logging
import pulsar


class Shelf(object):
    def __init__(self, task_id, topic_prefix):
        super(Shelf, self).__init__()
        self._task_id = task_id
        self._topic_url = f"{topic_prefix}/{self.task_id}"

    @property
    def task_id(self):
        return self._task_id

    @property
    def topic_url(self):
        return self._topic_url


class ShelfRoom(object):
    def __init__(self, pulsar_url, shelf_topic_prefix):
        super(ShelfRoom, self).__init__()

        self._pulsar_url = pulsar_url
        self._shelf_topic_prefix = shelf_topic_prefix
        self._client = pulsar.Client(self._pulsar_url)

        self.shelves = {}

        self._producers = {}
        self._consumers = {}

        self._subscription_name = "shelf-room"

    def add_shelf(self, task_id):
        logging.info("[Shelf Room] Adding shelf ...")
        if self.has_shelf(task_id):
            raise ValueError(f"Shelf for shelf {task_id} already exists")
        else:
            self.shelves[task_id] = Shelf(
                task_id, self._shelf_topic_prefix
            )

    def remove_shelf(self, task_id):
        logging.info("[Shelf Room] Removing shelf ...")
        if not self.has_shelf(task_id):
            raise ValueError(f"No such shelf: {task_id}")
        else:
            del self.shelves[task_id]

    def has_shelf(self, task_id):
        if task_id not in self.shelves:
            return False
        return True
            
    def get_shelf(self, task_id):
        logging.info("[Shelf Room] Getting shelf ...")
        if not self.has_shelf(task_id):
            raise ValueError(f"No such shelf: {task_id}")
        else:
            return self.shelves[task_id]

    def get_producer(self, task_id):
        if task_id in self._producers:
            return self._producers[task_id]

        shelf = self.get_shelf(task_id)

        producer = self._client.create_producer(
            topic=shelf.topic_url,
        )
        self._producers[task_id] = producer

        return producer

    def get_consumer(self, task_id):
        if task_id in self._consumers:
            return self._consumers[task_id]

        shelf = self.get_shelf(task_id)

        consumer = self._client.subscribe(
            shelf.topic_url,
            subscription_name=self._subscription_name,
        )

        self._consumers[task_id] = consumer

        return consumer

    def put_on_shelf(self, task_id, item):
        logging.info("[Shelf Room] Putting on shelf ...")
        logging.debug(f"[Shelf Room] item {item}")
        if task_id not in self.shelves:
            self.add_shelf(task_id)
        producer = self.get_producer(task_id)
        producer.send(item)

    def __str__(self):
        result = ""
        for task_id, shelf in self.shelves.items():
            result += f"Task {task_id}: {shelf}\n"
        return result


if __name__ == "__main__":
    import json
    from ols.test.gradient_house.config.gradient_house_config \
        import shelf_room_pulsar_url, shelf_room_topic_prefix

    logging.basicConfig(level=logging.INFO)

    shelf_room = ShelfRoom(shelf_room_pulsar_url, shelf_room_topic_prefix)

    message = {
        "task_id": "task01",
        "content": "I am a message"
    }
    message = json.dumps(message).encode('utf-8')

    shelf_room.put_on_shelf("task01", message)
    print(shelf_room)
    shelf_room.add_shelf("task02")
    print("add shelf", shelf_room)
    shelf_room.remove_shelf("task02")
    print("remove shelf", shelf_room)
