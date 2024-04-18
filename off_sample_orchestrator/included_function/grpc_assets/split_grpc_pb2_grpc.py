# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from . import split_grpc_pb2 as split__grpc__pb2


class SPLITRPCStub(object):
    """to compile
    python3 -m grpc_tools.protoc -I./ --python_out=. --grpc_python_out=. --pyi_out=. ./split_grpc.proto

    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Assign = channel.unary_unary(
                '/split_grpc.SPLITRPC/Assign',
                request_serializer=split__grpc__pb2.splitRequest.SerializeToString,
                response_deserializer=split__grpc__pb2.splitResponse.FromString,
                )


class SPLITRPCServicer(object):
    """to compile
    python3 -m grpc_tools.protoc -I./ --python_out=. --grpc_python_out=. --pyi_out=. ./split_grpc.proto

    """

    def Assign(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_SPLITRPCServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Assign': grpc.unary_unary_rpc_method_handler(
                    servicer.Assign,
                    request_deserializer=split__grpc__pb2.splitRequest.FromString,
                    response_serializer=split__grpc__pb2.splitResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'split_grpc.SPLITRPC', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class SPLITRPC(object):
    """to compile
    python3 -m grpc_tools.protoc -I./ --python_out=. --grpc_python_out=. --pyi_out=. ./split_grpc.proto

    """

    @staticmethod
    def Assign(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/split_grpc.SPLITRPC/Assign',
            split__grpc__pb2.splitRequest.SerializeToString,
            split__grpc__pb2.splitResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
