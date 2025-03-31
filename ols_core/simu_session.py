import os
import grpc
import time
import logging
import configparser

from concurrent import futures
from ols.resourceMgr.resource_manager import ResourceManager
from ols.taskMgr.task_manager import TaskManager
# from ols.rayclusterMgr.kuberay_cluster_manager import RayClusterManager
from ols.performanceMgr.performance_manager import PerformanceManager
from ols.proto import resourceService_pb2_grpc, taskService_pb2_grpc, rayclusterService_pb2_grpc, performanceService_pb2_grpc

# # log: Remove all handlers associated with the root logger object
# for handler in logging.root.handlers[:]:
#     logging.root.removeHandler(handler)

import os
from ols.simu_log import Logger
FILEPATH = os.path.abspath(os.path.dirname(__file__))   # 文件所在的绝对路径
logger = Logger()
logger.set_logger(log_yaml=FILEPATH + "/config/repo_log.yaml")


class SimulatorSession:
    def __init__(self, port, config_path, svc = 0):
        try:
            self.port = port
            self.config = configparser.ConfigParser()
            self.config.read(config_path)
            self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=[
            ('grpc.max_send_message_length', self.config.get("grpc", "max_send_message_length")),
            ('grpc.max_receive_message_length', self.config.get("grpc", "max_receive_message_length"))])
            if (svc == 0) or (svc == 1):
              self.task_mgr = TaskManager()
              taskService_pb2_grpc.add_TaskMgrServicer_to_server(self.task_mgr, self.server)
            if (svc == 0) or (svc == 2):
              self.resource_mgr = ResourceManager()
              resourceService_pb2_grpc.add_ResourceMgrServicer_to_server(self.resource_mgr, self.server)
            if (svc == 0) or (svc == 3):
              pass
              # self.ray_mgr = RayClusterManager()
              # rayclusterService_pb2_grpc.add_RayClusterMgrServicer_to_server(self.ray_mgr, self.server)
            if (svc == 0) or (svc == 4):
              self.performance_mgr = PerformanceManager()
              performanceService_pb2_grpc.add_PerformanceMgrServicer_to_server(self.performance_mgr, self.server)
            self.server.add_insecure_port(f'[::]:{self.port}')
            self.running_flag = False
        except Exception as e:
            logger.error(task_id="", system_name="TaskMgr", module_name="simu_session",
                         message=f"[__init__]: SimulatorSession initiation failed, because of {e}")
            raise Exception("SimulatorSession initiation failed.")

    def start(self):
        try:
            session_sleep_time = float(self.config.get("runtime", "session_sleep_time"))
        except Exception as e:
            logger.error(task_id="", system_name="TaskMgr", module_name="simu_session",
                         message=f"[start]: SimulatorSession start failed and session_sleep_time is invalid, because of {e}")
            raise Exception(f"SimulatorSession start failed and session_sleep_time is invalid, because of {e}")
        self.server.start()
        self.running_flag = True
        while self.running_flag:
            time.sleep(session_sleep_time)
        self.server.stop(0)


    def stop(self):
        self.running_flag = False


