import grpc
import mom_pb2
import mom_pb2_grpc

channel = grpc.insecure_channel("localhost:50051")
stub = mom_pb2_grpc.MessageServiceStub(channel)

# Send a message
response = stub.SendMessage(mom_pb2.MessageRequest(topic="Hola", message="Testing"))
print(response)