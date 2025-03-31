import logging

from ols.gradient_house.non_microservice.bound_room import InboundRoom
from ols.gradient_house.non_microservice.shelf_room import ShelfRoom
from ols.gradient_house.non_microservice.utils.message import parse_task_id


class Sorter(object):
    def __init__(self):
        super(Sorter, self).__init__()
        self._subscription_name = "sorter"

    def sort(self, inbound_room, shelf_room):
        logging.info("[Sorter] Begin sorting")

        consumer = inbound_room.get_consumer(self._subscription_name)
        logging.debug(f"[Sorter]  consumer {consumer}")
        while True:
            message = consumer.receive()
            message_value = message.value()
            try:
                task_id = parse_task_id(message_value)
                shelf_room.put_on_shelf(task_id, message_value)
            except KeyError as error:
                logging.info(f"[Sorter] Fail putting on shelf. Error in parsing task_id: {error}")
                pass
            except TypeError as error:
                logging.info(f"[Sorter] Fail putting on shelf. Error in parsing task_id: {error}")
                pass

            consumer.acknowledge(message)

        consumer.close()
        client.close()


if __name__ == "__main__":
    from ols.test.gradient_house.config.gradient_house_config \
    import shelf_room_pulsar_url, shelf_room_topic_prefix, inbound_pulsar_url, inbound_topic_url

    logging.basicConfig(level=logging.DEBUG)

    inbound_room = InboundRoom(
        inbound_pulsar_url, inbound_topic_url
    )

    shelf_room = ShelfRoom(
        shelf_room_pulsar_url,
        shelf_room_topic_prefix,
    )
    logging.info(f"shelf_room {shelf_room}")

    sorter = Sorter()
    sorter.sort(inbound_room, shelf_room)



