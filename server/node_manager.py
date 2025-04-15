import grpc
from concurrent import futures
import os
import sys
import socket
import redis
from server.global_topic import GlobalTopicRegistry
from server.state_manager import StateManager

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
from .grpc_generated import mom_pb2, mom_pb2_grpc

MASTER_NODE_PORT = os.getenv("MASTER_NODE_PORT", 60051)

class MasterNode(mom_pb2_grpc.MasterServiceServicer):
    def __init__(self):
        self.state_manager = StateManager()
        self.mom_instances = self.state_manager._load_state().get("mom_instances", {})
        self.current_instance = 0
        self.log_dir = "log"
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Redis setup
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = os.getenv("REDIS_PORT", 6379)
        self.redis = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)

    def register_master(self):
        """Register the master node in Redis and ensure no other masters exist."""
        master_key = "master_node"
        if self.redis.exists(master_key):
            print("[❌] Master node is already registered!")
            return False
        
        ip = self.get_local_ip()
        port = _find_free_port()
        #port = self.redis.get("master_node_port")
        master_address = f"{ip}:{port}"
        self.redis.set(master_key, master_address)
        print(f"[✅] Master node registered at {ip}:{port}")
        return {ip, port}

    def get_master_address(self):
        """Retrieve the master node address from Redis."""
        master_address = self.redis.get("master_node")
        if not master_address:
            raise Exception("Master node is not registered.")
        return master_address

    def unregister_master(self):
        """Unregister the master node from Redis."""
        self.redis.delete("master_node")
        print("[🧹] Master node unregistered.")

    def get_local_ip(self):
        """Get the local IP address of the machine."""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip

    def add_instance(self, ip_address=None):
        """Add a new MOM instance or register as the master node."""
        if not self.redis.exists("master_node"):
            # No master node exists, so this instance becomes the master
            registered = self.register_master()
            if registered:
                print("[✅] This instance is now the master node.")
                self.start_grpc_server(registered[0], registered[1])
                return
    
        # Register as a regular MOM instance
        hostname = ip_address or socket.gethostbyname(socket.gethostname())
        port = self._find_free_port()
        node_name = f"node-{len(self.mom_instances) + 1}"
    
        instance_address = f"{hostname}:{port}"
    
        # Avoid collisions if the address already exists
        if instance_address in self.mom_instances.values():
            print(f"⚠️ Instance {instance_address} already exists.")
            return
    
        self.mom_instances[node_name] = instance_address
        print(f"[✅] Instance {node_name} ({instance_address}) added to the cluster.")
        self._save_state()


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
        """Lógica de Round Robin"""
        if not self.mom_instances:
            raise Exception("No MOM instances available")

        node_names = list(self.mom_instances.keys())
        instance_name = node_names[self.current_instance]
        self.current_instance = (self.current_instance + 1) % len(node_names)

        hostname, port = self.mom_instances[instance_name].split(":")
        if hostname == socket.gethostname():
            hostname = "127.0.0.1"

        return instance_name, f"{hostname}:{port}"

    def GetNextInstance(self, request, context):
        """Método gRPC para enviar la siguiente instancia disponible"""
        try:
            name, address = self.get_next_instance()
            print(f"[MasterNode] Returning next instance: {name} ({address})")
            return mom_pb2.InstanceResponse(name=name, address=address)
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return mom_pb2.InstanceResponse()

    def log_message(self, topic, message, action):
        log_file = os.path.join(self.log_dir, "global_log.txt")
        with open(log_file, "a") as f:
            f.write(f"[{action}] Topic: {topic}, Message: {message}\n")

    def create_topic(self, topic_name, num_partitions):
        registry = GlobalTopicRegistry()
        registry.create_topic(topic_name, num_partitions)

    def _save_state(self):
        self.state_manager.update_state("mom_instances", self.mom_instances)

    def _find_free_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    def start_grpc_server(self, ip_address, port):
        if not self.register_master():
            print("⚠️ Aborting: Another master node is already running.")
            return

        #port = _find_free_port()
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        mom_pb2_grpc.add_MasterServiceServicer_to_server(self, server)
        server.add_insecure_port(f"[::]:{port}")
        print(f"[MasterNode] gRPC server starting on port {port}...")
        server.start()

        # Save the dynamically assigned port to Redis or a state file
        self.redis.set("master_node_port", port)

        # Ensure cleanup on exit
        import atexit
        atexit.register(self.unregister_master)

        try:
            server.wait_for_termination()
        except KeyboardInterrupt:
            print("[MasterNode] Shutting down gRPC server...")
            server.stop(0)