import grpc
import mom_pb2
import mom_pb2_grpc

GRPC_SERVER = "localhost:50051"

def send_message(topic, message):
    with grpc.insecure_channel(GRPC_SERVER) as channel:
        stub = mom_pb2_grpc.MessageServiceStub(channel)
        response = stub.SendMessage(mom_pb2.MessageRequest(topic=topic, message=message))
        print(response)

def receive_message(topic):
    with grpc.insecure_channel(GRPC_SERVER) as channel:
        stub = mom_pb2_grpc.MessageServiceStub(channel)
        response = stub.ReceiveMessage(mom_pb2.MessageRequest(topic=topic))
        print(response)

if __name__ == "__main__":
    send_message("my_topic", "Hello, MOM!")
    receive_message("my_topic")
