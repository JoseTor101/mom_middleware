from server.global_topic import GlobalTopicRegistry
import grpc
import time 
import sys
import os
# To solve the import error within the grpc_generated directory
sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
from .grpc_generated import mom_pb2
from .grpc_generated import mom_pb2_grpc


class MOMInstance(mom_pb2_grpc.MessageServiceServicer):
    def __init__(self, instance_name, redis_host='localhost', redis_port=6379):
        """Initialize the MOM instance."""
        self.instance_name = instance_name
        self.registry = GlobalTopicRegistry(redis_host, redis_port)

    def SendMessage(self, request, context):
        """Handle gRPC SendMessage requests."""
        print(f"[{self.instance_name}] Received message for topic '{request.topic}': {request.message}")
        self.registry.enqueue_message(request.topic, request.message)
        return mom_pb2.MessageResponse(status="Success", message="Message enqueued")

    def ReceiveMessage(self, request, context):
        """Handle gRPC ReceiveMessage requests."""
        print(f"[{self.instance_name}] Processing message for topic '{request.topic}'")
        message = self.registry.dequeue_message(request.topic, partition=0)  # Example: Always use partition 0
        return mom_pb2.MessageResponse(status="Success", message=message or "No messages available")

    def replicate_partition(self, topic_name, partition, target_instance):
        """Replicate a partition to another MOM instance."""
        partition_key = f"{topic_name}:partition{partition}"
        messages = self.registry.redis.lrange(partition_key, 0, -1)
        with grpc.insecure_channel(target_instance) as channel:
            stub = mom_pb2_grpc.MessageServiceStub(channel)
            for message in messages:
                stub.SendMessage(mom_pb2.MessageRequest(topic=topic_name, message=message))