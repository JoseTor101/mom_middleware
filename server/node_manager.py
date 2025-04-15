import redis
from collections import deque
import socket

class MasterNode:
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.instances = {}  # node_name -> (hostname, port)
        self.round_robin_queue = deque()  # Maintains node names for round-robin

    def add_instance(self, node_name, hostname, port):
        address = f"{hostname}:{port}"
        self.instances[node_name] = address
        if node_name not in self.round_robin_queue:
            self.round_robin_queue.append(node_name)

    def remove_instance(self, node_name):
        if node_name in self.instances:
            del self.instances[node_name]
        if node_name in self.round_robin_queue:
            self.round_robin_queue.remove(node_name)

    def list_instances(self):
        return self.instances

    def get_next_instance(self):
        if not self.round_robin_queue:
            raise Exception("No MOM instances registered")
        node_name = self.round_robin_queue.popleft()
        self.round_robin_queue.append(node_name)
        return node_name, self.instances[node_name]

    def create_topic(self, topic_name, num_partitions):
        for i in range(num_partitions):
            partition_key = f"{topic_name}:partition{i}"
            self.redis.delete(partition_key)  # Clean slate
            self.redis.rpush(partition_key, *[])  # Init empty list

    def log_message(self, topic, message, action="ENQUEUE"):
        log_key = f"{topic}:logs"
        self.redis.rpush(log_key, f"{action}: {message}")

    def resolve_address(self, hostname):
        try:
            return socket.gethostbyname(hostname)
        except socket.gaierror:
            raise Exception(f"Unable to resolve hostname: {hostname}")
