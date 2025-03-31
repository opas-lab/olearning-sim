"""任务队列

TaskQueue类用于管理向任务管理模块提交的任务

example:

task_queue = TaskQueue()
task_queue.add(task)  # 将任务添加到任务队列
task_queue_list = task_queue.getTaskQueue()  # 获取任务队列
task_ids = task_queue.getTaskIDsQueue()  # 获取任务ID列表
"""

from typing import List
from ols.proto import taskService_pb2

class TaskQueue:
    ''' 任务队列类

    用于管理向任务管理模块提交的任务

    属性:
        _task_queue: 列表, 任务提交队列
    '''
    def __init__(self):
        self._task_queue = []

    def add(self, taskconfig: taskService_pb2.TaskConfig) -> None:
        """向任务队列里增加任务"""
        self._task_queue.append(taskconfig)

    def getTaskQueue(self) -> List[taskService_pb2.TaskConfig]:
        """获取当前的任务队列"""
        return self._task_queue

    def setTaskQueue(
        self,
        task_queue: List[taskService_pb2.TaskConfig]
    ) -> None:
        """设置任务队列"""
        self._task_queue = task_queue

    def getTaskIDsQueue(self) -> List[taskService_pb2.TaskID]:
        """获取任务队列中所有任务的ID列表，供后端持久化使用"""
        taskIDsQueue = []
        task_queue = self._task_queue.copy()
        for task in task_queue:
            taskID = task.taskID
            taskIDsQueue.append(taskID)
        return taskIDsQueue


if __name__ == '__main__':
    task_queue = TaskQueue()
    task1 = taskService_pb2.TaskConfig(
        userID = "user1",
        taskID = taskService_pb2.TaskID(taskID="taskid1"),
    )
    task_queue.add(task1)
    task_queue_list = task_queue.getTaskQueue()
    task2 = taskService_pb2.TaskConfig(
        userID = "user2",
        taskID = taskService_pb2.TaskID(taskID="taskid2"),
    )
    task_queue.setTaskQueue([task1, task2])
    task_ids = task_queue.getTaskIDsQueue()
    task_ids_ground_truth = [
        taskService_pb2.TaskID(taskID="taskid1"),
        taskService_pb2.TaskID(taskID="taskid2"),
    ]
    assert task_ids == task_ids_ground_truth, (
        f"Expected task IDs {task_ids_ground_truth}, but got {task_ids}")
