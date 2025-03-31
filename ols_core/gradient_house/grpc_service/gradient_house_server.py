"""The Python implementation of the GRPC strategy_house.gradient_house server."""

from concurrent import futures
import logging

import grpc
import google.protobuf.empty_pb2
from ols.proto import gradient_house_pb2_grpc

from ols.gradient_house.non_microservice.gradient_house import GradientHouse


class GradientHouseServicer(gradient_house_pb2_grpc.GradientHouseServicer):
    def __init__(self,
                 inbound_pulsar_url: str,
                 inbound_topic_url: str,
                 outbound_pulsar_url: str,
                 outbound_topic_url: str,
                 shelf_room_pulsar_url: str,
                 shelf_room_topic_prefix: str,
                 ):
        self._gradient_house = GradientHouse(
            inbound_pulsar_url,
            inbound_topic_url,
            outbound_pulsar_url,
            outbound_topic_url,
            shelf_room_pulsar_url,
            shelf_room_topic_prefix,
        )

    def StartTask(self, request, context):
        task_id = request.task_id
        strategy_name = request.strategy_name
        self._gradient_house.start_task(task_id, strategy_name)
        response = google.protobuf.empty_pb2.Empty()
        return response

    def StopTask(self, request, context):
        task_id = request.task_id
        self._gradient_house.stop_task(task_id)
        response = google.protobuf.empty_pb2.Empty()
        return response


def serve(
        port: str,
        max_workers: int,
        inbound_pulsar_url: str,
        inbound_topic_url: str,
        outbound_pulsar_url: str,
        outbound_topic_url: str,
        shelf_room_pulsar_url: str,
        shelf_room_topic_prefix: str,
        ):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers))

    servicer = GradientHouseServicer(
        inbound_pulsar_url,
        inbound_topic_url,
        outbound_pulsar_url,
        outbound_topic_url,
        shelf_room_pulsar_url,
        shelf_room_topic_prefix,
    )

    gradient_house_pb2_grpc.add_GradientHouseServicer_to_server(servicer, server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    logging.info("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()

    from ols.test.gradient_house.config.gradient_house_config \
        import inbound_pulsar_url, inbound_topic_url, \
        outbound_pulsar_url, outbound_topic_url, \
        shelf_room_pulsar_url, shelf_room_topic_prefix, \
        port, max_workers

    serve(
        port,
        max_workers,
        inbound_pulsar_url,
        inbound_topic_url,
        outbound_pulsar_url,
        outbound_topic_url,
        shelf_room_pulsar_url,
        shelf_room_topic_prefix,
    )
