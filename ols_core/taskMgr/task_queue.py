import logging

class TaskQueue:
    def __init__(self):
        self._task_queue = list()

    def add(self, taskconfig):
        self._task_queue.append(taskconfig)

    def getTaskQueue(self):
        return self._task_queue

    def setTaskQueue(self, task_queue):
        self._task_queue = task_queue

    # 这个函数是给后端用的，后端会对taskconfig持久化，所以只提供序列化的taskID即可
    def getTaskIDsQueue(self):
        taskIDsQueue = list()
        task_queue = self._task_queue.copy()
        for task in task_queue:
            taskID = task.taskID
            taskIDsQueue.append(taskID)
        return taskIDsQueue






