import logging

from ols.gradient_house.non_microservice.shelf_room import ShelfRoom
from ols.gradient_house.non_microservice.bound_room import OutboundRoom

from ols.gradient_house.non_microservice.utils.observer import Observer

from ols.gradient_house.non_microservice.strategy import StrategyFactory


class Dispatcher(Observer):
    def __init__(self):
        Observer.__init__(self)
        self._strategies = {}

    def _get_strategy(self, strategy_name):
        if strategy_name not in self._strategies:
            strategy = StrategyFactory().create_strategy(strategy_name)
            self._strategies[strategy_name] = strategy
        else:
            strategy = self._strategies[strategy_name]
        return strategy

    def update(self, message, shelf_room, outbound_room):
        task_id, strategy_name = message
        self.dispatch(task_id, shelf_room, outbound_room, strategy_name)

    def dispatch(self, task_id, shelf_room: ShelfRoom, outbound_room: OutboundRoom, strategy_name: str):
        logging.info(f"[Dispatcher] Begin dispatching ...")
        if not shelf_room.has_shelf(task_id):
            logging.info(f"[Dispatcher] Finish dispatching. No shelf {task_id}.")
            return

        producer = outbound_room.get_producer()
        strategy = self._get_strategy(strategy_name)
        consumer = shelf_room.get_consumer(task_id)
        strategy.dispatch(consumer, producer)
        producer.close()
        logging.info(f"[Dispatcher] Finish dispatching")


if __name__ == "__main__":
    import json
    from ols.gradient_house.non_microservice.bound_room import OutboundRoom
    from ols.test.gradient_house.config.gradient_house_config \
    import outbound_pulsar_url, outbound_topic_url, shelf_room_pulsar_url, shelf_room_topic_prefix

    logging.basicConfig(level=logging.INFO)

    shelf_room = ShelfRoom(shelf_room_pulsar_url, shelf_room_topic_prefix)

    message = {
        "header": "i am a message",
        "data": {
            "task_id": "task01",
        }
    }
    message = json.dumps(message).encode('utf-8')

    shelf_room.put_on_shelf(
        "task01",
        message,
    )

    outbound_room = OutboundRoom(
        outbound_pulsar_url, outbound_topic_url
    )

    dispatcher = Dispatcher()
    dispatcher.dispatch(
        "task01",
        shelf_room,
        outbound_room,
        "sync"
    )