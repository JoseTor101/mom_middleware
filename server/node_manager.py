from server.global_topic import GlobalTopicRegistry
from server.state_manager import StateManager
import grpc
import sys
import os
import socket
sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
from .grpc_generated import mom_pb2
from .grpc_generated import mom_pb2_grpc

class MasterNode:
    def __init__(self, mom_instances=None):
        self.state_manager = StateManager()
        # Cargar estado anterior desde el archivo
        self.mom_instances = self.state_manager.load_state().get("mom_instances", {})
        self.current_instance = 0
        self.log_dir = "log"
        os.makedirs(self.log_dir, exist_ok=True)

    def add_instance(self, node_name=None, hostname=None, port=None):
        if hostname is None:
            hostname = socket.gethostname()
        if port is None:
            port = self._find_free_port()

        if node_name is None:
            node_name = f"node-{len(self.mom_instances) + 1}"

        instance_address = f"{hostname}:{port}"

        if node_name not in self.mom_instances:
            self.mom_instances[node_name] = instance_address
            print(f"Instance {node_name} ({instance_address}) added to the cluster.")
            self._save_state()
        else:
            print(f"Node {node_name} already exists in the cluster.")

    def remove_instance(self, node_name):
        if node_name in self.mom_instances:
            removed_address = self.mom_instances.pop(node_name)
            print(f"Instance {node_name} ({removed_address}) removed from the cluster.")
            self._save_state()
        else:
            print(f"Node {node_name} does not exist in the cluster.")

    def list_instances(self):
        return self.mom_instances

    def get_next_instance(self):
        if not self.mom_instances:
            raise Exception("No MOM instances available")
        node_names = list(self.mom_instances.keys())
        instance_name = node_names[self.current_instance]
        self.current_instance = (self.current_instance + 1) % len(node_names)
        hostname, port = self.mom_instances[instance_name].split(":")
        if hostname == socket.gethostname():
            hostname = "127.0.0.1"
        return instance_name, f"{hostname}:{port}"

    def _find_free_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    def log_message(self, topic, message, action):
        log_file = os.path.join(self.log_dir, "global_log.txt")
        with open(log_file, "a") as f:
            f.write(f"[{action}] Topic: {topic}, Message: {message}\n")

    def create_topic(self, topic_name, num_partitions):
        registry = GlobalTopicRegistry()
        registry.create_topic(topic_name, num_partitions)

    def _save_state(self):
        self.state_manager.update_state("mom_instances", self.mom_instances)
