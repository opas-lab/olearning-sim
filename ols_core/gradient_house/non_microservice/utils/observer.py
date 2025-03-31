class Subject(object):
    def __init__(self):
        super(Subject, self).__init__()
        self.observers = []

    def attach(self, observer):
        self.observers.append(observer)

    def detach(self, observer):
        self.observers.remove(observer)

    def notify(self, message, shelf_room, outbound_room):
        for observer in self.observers:
            observer.update(message, shelf_room, outbound_room)


class Observer(object):
    def __init__(self):
        super(Observer, self).__init__()
        self.subject = None

    def update(self, message, shelf_room, outbound_room):
        print(f"Observer received message: {message}")
