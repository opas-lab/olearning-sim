from abc import ABC, abstractmethod

from ols.ofl_commons.infrastructure.TaskRepo.task_domain import Task, TaskStatus


class TaskRepo(ABC):
    @abstractmethod
    def get_task(self, task_id: str) -> Task:
        pass

    @abstractmethod
    def set_status(self, task_id: str, task_status: TaskStatus):
        pass
