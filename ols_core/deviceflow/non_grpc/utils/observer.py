from ols.deviceflow.non_grpc.utils.exception import ShelfNotExistException


class Subject(object):
    def __init__(self):
        super(Subject, self).__init__()
        self.observers = []

    def attach(self, observer):
        self.observers.append(observer)

    def detach(self, observer):
        self.observers.remove(observer)

    def notify(self, message, shelf_room, target_url: str, target_topic: str):
        for observer in self.observers:
            observer.update(message, shelf_room, target_url, target_topic)


class Observer(object):
    def __init__(self):
        super(Observer, self).__init__()
        self.subject = None

    def update(self, message, shelf_room, target_url: str, target_topic: str):
        pass

    @staticmethod
    def _check_shelf_exist(task_id: str, shelf_room):
        """
        Check if the shelf exists for the given task.

        Args:
            task_id (str): The identifier of the task.

        Raises:
            ShelfNotExistException: If the shelf does not exist for the task.
        """
        try:
            if not shelf_room.has_shelf(task_id):
                raise ShelfNotExistException(task_id)
        except Exception as e:
            raise ShelfNotExistException(task_id) from e
