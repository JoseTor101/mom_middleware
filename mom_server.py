import grpc
from concurrent import futures
import mom_pb2
import mom_pb2_grpc

class MessageService(mom_pb2_grpc.MessageServiceServicer):
    def SendMessage(self, request, context):
        print(f"Received message: {request.message} on topic {request.topic}")
        return mom_pb2.MessageResponse(status="Success", message="Message received")

    def ReceiveMessage(self, request, context):
        return mom_pb2.MessageResponse(status="Success", message="No messages yet")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mom_pb2_grpc.add_MessageServiceServicer_to_server(MessageService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("MOM gRPC Server running on port 50051...")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
