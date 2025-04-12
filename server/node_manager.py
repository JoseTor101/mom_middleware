from server.global_topic import GlobalTopicRegistry
import grpc
import sys
import os
import socket
# To solve the import error within the grpc_generated directory
sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
from .grpc_generated import mom_pb2
from .grpc_generated import mom_pb2_grpc

class MasterNode:
    def __init__(self, mom_instances=None):
        # Use a dictionary to map node names to their addresses
        self.mom_instances = mom_instances or {}
        self.current_instance = 0
        self.log_dir = "log"  # Directory to store logs
        os.makedirs(self.log_dir, exist_ok=True)  # Ensure the log directory exists

    def add_instance(self, node_name=None, hostname=None, port=None):
        """Register a MOM node in the cluster."""
        # If no hostname or port is provided, use default values
        if hostname is None:
            hostname = socket.gethostname()
        if port is None:
            port = self._find_free_port()

        # Generate a unique name for the node if not provided
        if node_name is None:
            node_name = f"node-{len(self.mom_instances) + 1}"

        instance_address = f"{hostname}:{port}"

        if node_name not in self.mom_instances:
            self.mom_instances[node_name] = instance_address
            print(f"Instance {node_name} ({instance_address}) added to the cluster.")
        else:
            print(f"Node {node_name} already exists in the cluster.")

    def remove_instance(self, node_name):
        """Remove a MOM node from the cluster by its name."""
        if node_name in self.mom_instances:
            removed_address = self.mom_instances.pop(node_name)
            print(f"Instance {node_name} ({removed_address}) removed from the cluster.")
        else:
            print(f"Node {node_name} does not exist in the cluster.")

    def list_instances(self):
        """List all MOM nodes in the cluster."""
        return self.mom_instances

    def get_next_instance(self):
        """Get the next MOM node in the cluster (round-robin)."""
        if not self.mom_instances:
            raise Exception("No MOM instances available")
        node_names = list(self.mom_instances.keys())
        instance_name = node_names[self.current_instance]
        self.current_instance = (self.current_instance + 1) % len(node_names)
        hostname, port = self.mom_instances[instance_name].split(":")
        if hostname == socket.gethostname():
            hostname = "127.0.0.1"  # Use IPv4 loopback for local connections
        return instance_name, f"{hostname}:{port}"

    def _find_free_port(self):
        """Find an available port on the current machine."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))  # Bind to any available port
            return s.getsockname()[1]  # Return the port number

    def log_message(self, topic, message, action):
        """Log a message to a global log file."""
        log_file = os.path.join(self.log_dir, "global_log.txt")
        with open(log_file, "a") as f:
            f.write(f"[{action}] Topic: {topic}, Message: {message}\n")

    def create_topic(self, topic_name, num_partitions):
        """Create a new topic with the given number of partitions."""
        registry = GlobalTopicRegistry()
        registry.create_topic(topic_name, num_partitions)