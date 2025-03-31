import json


def parse_task_id(message):
    message = json.loads(message)
    data = message["data"]
    task_id = data["task_id"]
    return task_id


if __name__ == "__main__":
    message = {
        "header": "i am a message",
        "data": {
            "task_id": "task1",
        }
    }
    message = json.dumps(message).encode('utf-8')

    task_id = parse_task_id(message)
    print(task_id)




