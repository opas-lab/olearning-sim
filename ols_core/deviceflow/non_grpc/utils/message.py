import json


def parse_routing_key(message):
    message = json.loads(message)
    routing_key = message["routing_key"]
    return routing_key


def parse_message_content(message):
    message = json.loads(message)
    message_content = message["message"]
    return message_content


def parse_compute_resource(message):
    message = json.loads(message)
    routing_key = message["compute_resource"]
    return routing_key


if __name__ == "__main__":
    message = {
        "header": "i am a message",
        "data": {
            "task_id": "task1",
        }
    }
    message = json.dumps(message).encode('utf-8')

    routing_key = parse_routing_key(message)
    print(routing_key)
