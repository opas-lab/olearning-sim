import yaml


def get_grpc_service_address(path):
    with open(path, encoding="utf-8") as f:
        content = f.read()
        data = yaml.load(content, Loader=yaml.FullLoader)
        grpc_service_address = data["grpc_service_address"]
    return grpc_service_address


def get_pulsar_url_of_outbound_room(path):
    with open(path, encoding="utf-8") as f:
        content = f.read()
        data = yaml.load(content, Loader=yaml.FullLoader)
        outbound_pulsar_url = data["service_url"]
    return outbound_pulsar_url


def get_topic_url_of_outbound_room(path):
    with open(path, encoding="utf-8") as f:
        content = f.read()
        data = yaml.load(content, Loader=yaml.FullLoader)
        outbound_topic_url = data["pulsar_topic"][0]
    return outbound_topic_url


def get_subscription_of_outbound_room(path):
    with open(path, encoding="utf-8") as f:
        content = f.read()
        data = yaml.load(content, Loader=yaml.FullLoader)
        outbound_subscription = data["subscription"]
    return outbound_subscription


def get_source_url(path):
    with open(path, encoding="utf-8") as f:
        content = f.read()
        data = yaml.load(content, Loader=yaml.FullLoader)
        pulsar_url = data["source_url"]
    return pulsar_url


def get_source_topic(path):
    with open(path, encoding="utf-8") as f:
        content = f.read()
        data = yaml.load(content, Loader=yaml.FullLoader)
        source_topic = data["source_topic"]
    return source_topic


def get_port(path):
    with open(path, encoding="utf-8") as f:
        content = f.read()
        data = yaml.load(content, Loader=yaml.FullLoader)
        port = data["port"]
    return port


def get_max_workers(path):
    with open(path, encoding="utf-8") as f:
        content = f.read()
        data = yaml.load(content, Loader=yaml.FullLoader)
        max_workers = data["max_workers"]
    return max_workers


def get_shelfroom_url(path):
    with open(path, encoding="utf-8") as f:
        content = f.read()
        data = yaml.load(content, Loader=yaml.FullLoader)
        shelfroom_url = data["shelfroom_url"]
    return shelfroom_url


def get_shelfroom_namespace(path):
    with open(path, encoding="utf-8") as f:
        content = f.read()
        data = yaml.load(content, Loader=yaml.FullLoader)
        shelfroom_namespace = data["shelfroom_namespace"]
    return shelfroom_namespace








