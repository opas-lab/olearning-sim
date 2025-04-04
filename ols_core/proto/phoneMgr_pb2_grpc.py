# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
import phoneMgr_pb2 as phoneMgr__pb2
import taskService_pb2 as taskService__pb2


class TaskManagerStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.submitTask = channel.unary_unary(
                '/TaskManager/submitTask',
                request_serializer=taskService__pb2.DeviceTaskConfig.SerializeToString,
                response_deserializer=phoneMgr__pb2.ActionStatus.FromString,
                )
        self.getDeviceAvailableResource = channel.unary_unary(
                '/TaskManager/getDeviceAvailableResource',
                request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
                response_deserializer=phoneMgr__pb2.AllUsersDeviceAvailableResource.FromString,
                )
        self.requestDeviceResource = channel.unary_unary(
                '/TaskManager/requestDeviceResource',
                request_serializer=phoneMgr__pb2.DeviceResource.SerializeToString,
                response_deserializer=phoneMgr__pb2.ActionStatus.FromString,
                )
        self.releaseDeviceResource = channel.unary_unary(
                '/TaskManager/releaseDeviceResource',
                request_serializer=taskService__pb2.TaskID.SerializeToString,
                response_deserializer=phoneMgr__pb2.ActionStatus.FromString,
                )
        self.stopDevice = channel.unary_unary(
                '/TaskManager/stopDevice',
                request_serializer=taskService__pb2.TaskID.SerializeToString,
                response_deserializer=phoneMgr__pb2.ActionStatus.FromString,
                )
        self.getDeviceTaskStatus = channel.unary_unary(
                '/TaskManager/getDeviceTaskStatus',
                request_serializer=taskService__pb2.TaskID.SerializeToString,
                response_deserializer=phoneMgr__pb2.DeviceTaskResult.FromString,
                )


class TaskManagerServicer(object):
    """Missing associated documentation comment in .proto file."""

    def submitTask(self, request, context):
        """提交任务
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def getDeviceAvailableResource(self, request, context):
        """查询资源
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def requestDeviceResource(self, request, context):
        """冻结资源
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def releaseDeviceResource(self, request, context):
        """释放资源
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def stopDevice(self, request, context):
        """任务停止
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def getDeviceTaskStatus(self, request, context):
        """查询任务状态
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_TaskManagerServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'submitTask': grpc.unary_unary_rpc_method_handler(
                    servicer.submitTask,
                    request_deserializer=taskService__pb2.DeviceTaskConfig.FromString,
                    response_serializer=phoneMgr__pb2.ActionStatus.SerializeToString,
            ),
            'getDeviceAvailableResource': grpc.unary_unary_rpc_method_handler(
                    servicer.getDeviceAvailableResource,
                    request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
                    response_serializer=phoneMgr__pb2.AllUsersDeviceAvailableResource.SerializeToString,
            ),
            'requestDeviceResource': grpc.unary_unary_rpc_method_handler(
                    servicer.requestDeviceResource,
                    request_deserializer=phoneMgr__pb2.DeviceResource.FromString,
                    response_serializer=phoneMgr__pb2.ActionStatus.SerializeToString,
            ),
            'releaseDeviceResource': grpc.unary_unary_rpc_method_handler(
                    servicer.releaseDeviceResource,
                    request_deserializer=taskService__pb2.TaskID.FromString,
                    response_serializer=phoneMgr__pb2.ActionStatus.SerializeToString,
            ),
            'stopDevice': grpc.unary_unary_rpc_method_handler(
                    servicer.stopDevice,
                    request_deserializer=taskService__pb2.TaskID.FromString,
                    response_serializer=phoneMgr__pb2.ActionStatus.SerializeToString,
            ),
            'getDeviceTaskStatus': grpc.unary_unary_rpc_method_handler(
                    servicer.getDeviceTaskStatus,
                    request_deserializer=taskService__pb2.TaskID.FromString,
                    response_serializer=phoneMgr__pb2.DeviceTaskResult.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'TaskManager', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class TaskManager(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def submitTask(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/TaskManager/submitTask',
            taskService__pb2.DeviceTaskConfig.SerializeToString,
            phoneMgr__pb2.ActionStatus.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def getDeviceAvailableResource(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/TaskManager/getDeviceAvailableResource',
            google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            phoneMgr__pb2.AllUsersDeviceAvailableResource.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def requestDeviceResource(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/TaskManager/requestDeviceResource',
            phoneMgr__pb2.DeviceResource.SerializeToString,
            phoneMgr__pb2.ActionStatus.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def releaseDeviceResource(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/TaskManager/releaseDeviceResource',
            taskService__pb2.TaskID.SerializeToString,
            phoneMgr__pb2.ActionStatus.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def stopDevice(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/TaskManager/stopDevice',
            taskService__pb2.TaskID.SerializeToString,
            phoneMgr__pb2.ActionStatus.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def getDeviceTaskStatus(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/TaskManager/getDeviceTaskStatus',
            taskService__pb2.TaskID.SerializeToString,
            phoneMgr__pb2.DeviceTaskResult.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)


class PhoneManagerStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.connectMSPDevices = channel.unary_unary(
                '/PhoneManager/connectMSPDevices',
                request_serializer=phoneMgr__pb2.MSPDevicesConfig.SerializeToString,
                response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
                )
        self.getLocalDeviceResource = channel.unary_unary(
                '/PhoneManager/getLocalDeviceResource',
                request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
                response_deserializer=phoneMgr__pb2.DevicesInfo.FromString,
                )


class PhoneManagerServicer(object):
    """Missing associated documentation comment in .proto file."""

    def connectMSPDevices(self, request, context):
        """连接MSP真机池
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def getLocalDeviceResource(self, request, context):
        """获取本地连接设备信息
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_PhoneManagerServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'connectMSPDevices': grpc.unary_unary_rpc_method_handler(
                    servicer.connectMSPDevices,
                    request_deserializer=phoneMgr__pb2.MSPDevicesConfig.FromString,
                    response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            ),
            'getLocalDeviceResource': grpc.unary_unary_rpc_method_handler(
                    servicer.getLocalDeviceResource,
                    request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
                    response_serializer=phoneMgr__pb2.DevicesInfo.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'PhoneManager', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class PhoneManager(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def connectMSPDevices(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/PhoneManager/connectMSPDevices',
            phoneMgr__pb2.MSPDevicesConfig.SerializeToString,
            google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def getLocalDeviceResource(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/PhoneManager/getLocalDeviceResource',
            google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            phoneMgr__pb2.DevicesInfo.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
