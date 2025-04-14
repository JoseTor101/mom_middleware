from server.global_topic import GlobalTopicRegistry
import grpc
import time 
import sys
import os
import requests
import socket
# To solve the import error within the grpc_generated directory
sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
from .grpc_generated import mom_pb2
from .grpc_generated import mom_pb2_grpc


class MOMInstance(mom_pb2_grpc.MessageServiceServicer):
    def __init__(self, instance_name, master_node):
        """Initialize the MOM instance."""
        self.instance_name = instance_name
        self.master_node = master_node  # Reference to the MasterNode

    def register_with_master_node(self):
        """Register a node at the master node."""
        hostname = socket.gethostname()
        port = self._find_free_port()
        response = requests.post(
            f"{self.master_node_url}/nodes/register",
            json={"node_name": self.instance_name, "hostname": hostname, "port": port}
        )
        if response.status_code == 200:
            print(f"[{self.instance_name}] Successfully registered with MasterNode.")
        else:
            print(f"[{self.instance_name}] Failed to register with MasterNode: {response.text}")

    def send_message_to_topic(self, topic_name, message):
        """Send a message to a topic using gRPC."""
        print(f"[{self.instance_name}] Sending message to topic '{topic_name}': {message}")
        instance_name, instance_address = self.master_node.get_next_instance()
        with grpc.insecure_channel(instance_address) as channel:
            stub = mom_pb2_grpc.MessageServiceStub(channel)
            response = stub.SendMessage(mom_pb2.MessageRequest(topic=topic_name, message=message))
        return response

    def SendMessage(self, request, context):
        """Handle gRPC SendMessage requests."""
        print(f"[{self.instance_name}] Received message for topic '{request.topic}': {request.message}")

        registry = GlobalTopicRegistry()
        registry.enqueue_message(request.topic, request.message)

        self.master_node.log_message(request.topic, request.message, action="ENQUEUE")
        return mom_pb2.MessageResponse(status="Success", message="Message enqueued")

    def ReceiveMessage(self, request, context):
        """Handle gRPC ReceiveMessage requests."""
        print(f"[{self.instance_name}] Processing message for topic '{request.topic}'")
        message = "Example message"  # Replace with actual dequeue logic
        self.master_node.log_message(request.topic, message, action="DEQUEUE")
        return mom_pb2.MessageResponse(status="Success", message=message or "No messages available")
        
    def replicate_partition(self, topic_name, partition, target_instance):
        """Replicate a partition to another MOM instance."""
        partition_key = f"{topic_name}:partition{partition}"
        messages = self.registry.redis.lrange(partition_key, 0, -1)
        with grpc.insecure_channel(target_instance) as channel:
            stub = mom_pb2_grpc.MessageServiceStub(channel)
            for message in messages:
                stub.SendMessage(mom_pb2.MessageRequest(topic=topic_name, message=message))