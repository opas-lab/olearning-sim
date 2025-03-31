import json
import pydantic

from ols.ofl_commons.infrastructure.TaskRepo.task_domain import Task, TaskStatus
from ols.ofl_commons.infrastructure.TaskRepo.task_repo import TaskRepo

class TaskRepoMock(TaskRepo):
    def __init__(self):
        self._task = {}
        with open('task_mock.json', 'r') as f:
            task_dict = json.load(f)
        self._task[task_dict['data']["id"]] = pydantic.parse_obj_as(Task, task_dict['data'])

    def get_task(self, task_id: str) -> Task:
        return self._task[task_id]

    def set_status(self, task_id: str, task_status: TaskStatus):
        self._task[task_id].status = task_status

    def set_task(self, task: Task):
        self._task[task.id] = task

if __name__ == '__main__':
    from ofl_server.domain.task import TaskAcc
    with open('task_mock.json', 'r') as f:
        task_dict = json.load(f)
    
    task_id = task_dict['data']['id']
    task_repo = TaskRepoMock()
    task1 = task_repo.get_task(task_id=task_id)
    print(task1.json(indent=2))
    current_acc = TaskAcc(
        train_acc=0.93,
        train_auc=0.88,
        train_loss=0.22
    )

    task_repo.set_status(
        task_id=task_id,
        task_status=TaskStatus(
            current_acc=current_acc
        )
    )
    task1 = task_repo.get_task(task_id=task_id)
    print(task1.json(indent=2))
