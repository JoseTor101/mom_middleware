import grpc
import sys
import os
# To solve the import error within the grpc_generated directory
sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
from .grpc_generated import mom_pb2
from .grpc_generated import mom_pb2_grpc

channel = grpc.insecure_channel("127.0.0.1:52774")  # Replace with your target address
stub = mom_pb2_grpc.MessageServiceStub(channel)

try:
    response = stub.SendMessage(mom_pb2.MessageRequest(topic="test", message="Hello, MOM!"))
    print("Response:", response)
except grpc.RpcError as e:
    print("gRPC Error:", e)