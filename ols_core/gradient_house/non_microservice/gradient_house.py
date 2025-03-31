import logging
import threading

from abc import ABC, abstractmethod

import pulsar

from ols.gradient_house.non_microservice.bound_room import InboundRoom, OutboundRoom
from ols.gradient_house.non_microservice.shelf_room import ShelfRoom
from ols.gradient_house.non_microservice.dispatcher import Dispatcher
from ols.gradient_house.non_microservice.sorter import Sorter
from ols.gradient_house.non_microservice.utils.observer import Subject
from ols.gradient_house.non_microservice.utils.strategy import dispatch_after_signal, dispatch_self_driven
from ols.test.gradient_house.utils.error import NoStrategyError


class StrategyHouseBase(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def start_task(self, task_id: str, strategy_name: str):
        pass

    @abstractmethod
    def stop_task(self, task_id: str):
        pass


class GradientHouse(StrategyHouseBase, Subject):
    def __init__(self,
                 inbound_pulsar_url: str,
                 inbound_topic_url: str,
                 outbound_pulsar_url: str,
                 outbound_topic_url: str,
                 shelf_room_pulsar_url: str,
                 shelf_room_topic_prefix: str,
                 ):
        StrategyHouseBase.__init__(self)
        Subject.__init__(self)

        self._inbound_room = InboundRoom(
            inbound_pulsar_url, inbound_topic_url
        )

        self._outbound_room = OutboundRoom(
            outbound_pulsar_url, outbound_topic_url
        )

        self._shelf_room = ShelfRoom(shelf_room_pulsar_url, shelf_room_topic_prefix)

        self._sorter = Sorter()
        self._dispatcher = Dispatcher()

        self.attach(self._dispatcher)

        self._strategies = {}

        t = threading.Thread(
            target=self._sorter.sort,
            args=(self._inbound_room, self._shelf_room))
        t.start()

    def start_task(self, task_id: str, strategy_name: str):
        logging.info(f"[Gradient House] Start task {task_id} with {strategy_name}")
        self._strategies[task_id] = strategy_name
        if strategy_name in dispatch_self_driven:
            message = (task_id, strategy_name)
            self.notify(message, self._shelf_room, self._outbound_room)

    def _pop_strategy_name(self, task_id) -> str:
        if task_id not in self._strategies:
            raise NoStrategyError(f"No strategy saved for {task_id}")

        strategy_name = self._strategies.pop(task_id)
        return strategy_name

    def stop_task(self, task_id: str):
        logging.info(f"[Gradient House] Stop task {task_id}")
        try:
            strategy_name = self._pop_strategy_name(task_id)
        except NoStrategyError as error:
            raise ValueError(
                f"[Gradient House] You may not start the task, "
                f"you should start the task first."
                f"{error}"
                )

        if strategy_name in dispatch_after_signal:
            message = (task_id, strategy_name)
            self.notify(message, self._shelf_room, self._outbound_room)








