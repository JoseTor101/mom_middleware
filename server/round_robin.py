from server.global_topic import GlobalTopicRegistry
import grpc
import sys
import os
# To solve the import error within the grpc_generated directory
sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
from .grpc_generated import mom_pb2
from .grpc_generated import mom_pb2_grpc

class MasterNode:
    def __init__(self, mom_instances):
        """Initialize the master node with MOM instances."""
        self.registry = GlobalTopicRegistry()
        self.mom_instances = mom_instances
        self.current_index = 0

    def get_next_instance(self):
        """Get the next MOM instance using round-robin."""
        instance = self.mom_instances[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.mom_instances)
        return instance

    def list_instances(self):
        """List all MOM instances."""
        return self.mom_instances
    
    def send_message(self, topic_name, message):
        """Distribute a message to a MOM instance."""
        instance = self.get_next_instance()
        print(f"Dispatching message to MOM instance: {instance}")
        with grpc.insecure_channel(instance) as channel:
            stub = mom_pb2_grpc.MessageServiceStub(channel)
            response = stub.SendMessage(mom_pb2.MessageRequest(topic=topic_name, message=message))
            print(f"Response from {instance}: {response.status}")

    def create_topic(self, topic_name, num_partitions=3):
        """Create a topic with partitions."""
        self.registry.create_topic(topic_name, num_partitions)