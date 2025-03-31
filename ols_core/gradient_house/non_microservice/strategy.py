import logging
import time
from abc import ABC, abstractmethod

import _pulsar


class StrategyBase(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def dispatch(self, container, sender):
        pass


class SynchronousStrategy(StrategyBase):
    name = "sync"

    def __init__(self):
        super(SynchronousStrategy, self).__init__()

    def dispatch(self, receiver_from_source, sender_to_target):
        logging.info(f"[Strategy] Begin dispatching ...")

        while True:
            try:
                msg = receiver_from_source.receive(timeout_millis=1000)
                message_value = msg.value()
                sender_to_target.send(message_value)
                receiver_from_source.acknowledge(msg)
            except _pulsar.Timeout as error:
                logging.debug(f"[Strategy] {error}")
                break


class AsynchronousStrategy(StrategyBase):
    name = "async"

    def __init__(self):
        super(AsynchronousStrategy, self).__init__()

    def dispatch(self, container, sender):
        pass


class StrategyFactory(object):
    def __init__(self):
        self.strategy_dict = {
            SynchronousStrategy.name: SynchronousStrategy,
            AsynchronousStrategy.name: AsynchronousStrategy
        }

    def create_strategy(self, strategy_name: str) -> StrategyBase:
        strategy_class = self.strategy_dict.get(strategy_name)
        if strategy_class:
            return strategy_class()
        else:
            raise ValueError(f"No such strategy: {strategy_name}")
