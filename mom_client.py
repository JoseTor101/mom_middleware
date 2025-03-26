import grpc
import mom_pb2
import mom_pb2_grpc

def send_message(topic, message):
    channel = grpc.insecure_channel("localhost:50051")
    stub = mom_pb2_grpc.MessageServiceStub(channel)
    response = stub.SendMessage(mom_pb2.MessageRequest(topic=topic, message=message))
    print(f"Server Response: {response.status} - {response.message}")

if __name__ == "__main__":
    send_message("test_topic", "Hello from Client!")
