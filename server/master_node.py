import os
import socket
import sys
import time
from concurrent import futures

import grpc
import redis

from server.global_topic import GlobalTopicRegistry
from server.state_manager import StateManager
from server.mom_instance import MOMInstance
from utils.utils import get_local_ip, get_public_ip, check_port_externally_accessible, find_free_port
sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
from .grpc_generated import mom_pb2, mom_pb2_grpc


class MasterNode(mom_pb2_grpc.MasterServiceServicer, mom_pb2_grpc.MessageServiceServicer):
    def __init__(self):
        self.state_manager = StateManager()
        self.mom_instances = self.state_manager._load_state().get("mom_instances", {})
        self.current_instance = 0
        self.log_dir = "log"
        os.makedirs(self.log_dir, exist_ok=True)

        # Redis setup
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.redis = redis.Redis(
            host=self.redis_host, port=self.redis_port, decode_responses=True
        )

        self.public_address = None
        self.registry = GlobalTopicRegistry(self.redis_host, self.redis_port)
        self.grpc_port = None
        self.instance_name = "master-node"
        
        # Set auto_remove to True if you want it to automatically clean up dead nodes
        self.start_health_check_thread(check_interval=60, auto_remove=True)
        self.start_heartbeat_thread()

    def register_master(self):
        """Register the master node in Redis and ensure no other masters exist."""
        master_key = "master_node"
        if self.redis.exists(master_key):
            print("[❌] Master node is already registered!")
            return False, None, None

        # Get local IP for internal communication
        local_ip = get_local_ip()
        grpc_port = find_free_port()
        self.grpc_port = grpc_port
        
        # Get public IP for external machine connections
        public_ip = get_public_ip()
        
        # Store both addresses
        master_grpc_address = f"{local_ip}:{grpc_port}"
        self.public_address = f"{public_ip}:{grpc_port}"
        
        # Store in Redis for discovery
        self.redis.set(master_key, master_grpc_address)
        self.redis.set("master_node_public", self.public_address)
        self.redis.set("master_node_port", grpc_port)
        
        # Register ourselves as the first MOM instance
        self.mom_instances[self.instance_name] = master_grpc_address
        self._save_state()
        
        print(f"[✅] Master node registered at {local_ip}:{grpc_port}")
        print(f"[✅] Public address: {self.public_address}")
        print(f"[🌐] External machines can connect using: {self.public_address}")
        print(f"[✅] Master node is also registered as MOM instance: {self.instance_name}")
        
        return True, local_ip, grpc_port

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

    def update_heartbeat(self):
        """Update the master node heartbeat in Redis."""
        try:
            # Use a Redis key with TTL for automatic expiration
            self.redis.set("master_node_heartbeat", "alive", ex=10)  # 10 second TTL
        except Exception as e:
            print(f"[MasterNode] Error updating heartbeat: {e}")
        
    def start_heartbeat_thread(self):
        """Start a background thread that periodically updates the master heartbeat."""
        import threading
        import time
        
        def heartbeat_worker():
            while True:
                try:
                    self.update_heartbeat()
                except Exception as e:
                    print(f"[MasterNode] Error in heartbeat thread: {e}")
                time.sleep(5)  # Update heartbeat every 5 seconds
        
        thread = threading.Thread(target=heartbeat_worker, daemon=True)
        thread.start()
        print(f"[MasterNode] Started master heartbeat thread")

    def add_instance(self, ip_address=None):
        """Add a new MOM instance or register as the master node."""
        if not self.redis.exists("master_node"):
            # No master node exists, so this instance becomes the master
            registered = self.register_master()
            if registered:
                print("[✅] This instance is now the master node.")
                self.start_grpc_server(registered[1], registered[2])
                return

        # Register as a regular MOM instance
        hostname = ip_address or socket.gethostbyname(socket.gethostname())
        port = find_free_port()
        node_name = f"node-{len(self.mom_instances) + 1}"

        instance_address = f"{hostname}:{port}"

        # Avoid collisions if the address already exists
        if instance_address in self.mom_instances.values():
            print(f"⚠️ Instance {instance_address} already exists.")
            return

        self.mom_instances[node_name] = instance_address
        print(
            f"[✅] Instance {node_name} ({instance_address}) added to the cluster.")
        self._save_state()

        mom_instance = MOMInstance(node_name, self.get_master_address(), port)
        mom_instance.start_server()

    def remove_instance(self, node_name):
        """Remove a MOM instance from the cluster."""
        if node_name in self.mom_instances:
            removed_address = self.mom_instances.pop(node_name)
            print(
                f"Instance {node_name} ({removed_address}) removed from the cluster.")
            self._save_state()
        else:
            print(f"Node {node_name} does not exist in the cluster.")

    def list_instances(self):
        """List all registered MOM instances."""
        return self.mom_instances

    def get_next_instance(self):
        """Get the next available MOM instance in round-robin order."""
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
        """ gRPC method to send the next available instance """
        try:
            name, address = self.get_next_instance()
            print(f"[MasterNode] Returning next instance: {name} ({address})")
            return mom_pb2.InstanceResponse(name=name, address=address)
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return mom_pb2.InstanceResponse()

    def RegisterMOMInstance(self, request, context):
        """ gRPC method to register a MOM instance with the master node """
        try:
            node_name = request.node_name
            hostname = request.hostname
            port = request.port
            
            instance_address = f"{hostname}:{port}"
            
            # Verify if the instance is already registered
            if instance_address in self.mom_instances.values():
                print(f"[MasterNode] Instance already exists: {instance_address}")
                return mom_pb2.MessageResponse(
                    status="Error", 
                    message=f"Instance already exists at {instance_address}"
                )
                
            # Generate a unique node name if it already exists
            if node_name in self.mom_instances:
                node_name = f"{node_name}-{len(self.mom_instances) + 1}"
                
            # Register the new instance
            self.mom_instances[node_name] = instance_address
            self._save_state()
            print(f"[MasterNode] Registered new instance: {node_name} at {instance_address}")
            
            return mom_pb2.MessageResponse(
                status="Success", 
                message=f"Instance {node_name} registered successfully"
            )
        except Exception as e:
            print(f"[MasterNode] Error registering instance: {e}")
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mom_pb2.MessageResponse(
                status="Error", 
                message=f"Failed to register instance: {str(e)}"
            )

    def log_message(self, topic, message, action):
        """Log messages to a file."""
        log_file = os.path.join(self.log_dir, "global_log.txt")
        with open(log_file, "a") as f:
            f.write(f"[{action}] Topic: {topic}, Message: {message}\n")

    def create_topic(self, topic_name, num_partitions):
        """Create a new topic and broadcast to all MOM instances."""
        try:
            # Create locally first
            registry = GlobalTopicRegistry()
            registry.create_topic(topic_name, num_partitions)
            
            # Notify all instances about the new topic
            for node_name, address in self.mom_instances.items():
                try:
                    with grpc.insecure_channel(address) as channel:
                        stub = mom_pb2_grpc.MessageServiceStub(channel)
                        response = stub.CreateTopic(
                            mom_pb2.TopicRequest(
                                topic_name=topic_name, 
                                partitions=num_partitions
                            ))
                        print(f"[MasterNode] Topic {topic_name} created on {node_name}")
                except Exception as e:
                    print(f"[MasterNode] Failed to create topic on {node_name}: {e}")
                    
        except Exception as e:
            print(f"Error creating topic: {e}")
            raise

    def CreateTopic(self, request, context):
        """Create a new topic with the specified number of partitions."""
        try:
            self.registry.create_topic(request.topic_name, request.partitions)
            return mom_pb2.MessageResponse(
                status="Success", 
                message=f"Topic {request.topic_name} created with {request.partitions} partitions"
            )
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mom_pb2.MessageResponse(
                status="Error", 
                message=f"Failed to create topic: {str(e)}"
            )
            
    def SendMessage(self, request, context):
        """Send a message to the specified topic."""
        print(f"[{self.instance_name}] Received message for topic '{request.topic}': {request.message}")
        
        # Check if topic exists, if not create it with default partitions
        topic_exists = self.registry.redis.sismember("topics", request.topic)
        if not topic_exists:
            print(f"[{self.instance_name}] Topic '{request.topic}' doesn't exist, creating with default partitions")
            self.registry.create_topic(request.topic, 3)  # Create with default 3 partitions
        
        self.registry.enqueue_message(request.topic, request.message)
        return mom_pb2.MessageResponse(
            status="Success", message="Message enqueued")

    def ReceiveMessage(self, request, context):
        """Receive a message from the specified topic."""
        print(
            f"[{self.instance_name}] Processing message for topic '{request.topic}'")
        partition_count = self.registry.get_partition_count(request.topic)
        message = None

        if partition_count > 0:
            for i in range(partition_count):
                message = self.registry.dequeue_message(request.topic, i)
                if message:
                    break

        if message:
            return mom_pb2.MessageResponse(status="Success", message=message)
        else:
            return mom_pb2.MessageResponse(
                status="Empty", message="No messages available"
            )

    def _save_state(self):
        self.state_manager.update_state("mom_instances", self.mom_instances)

    def start_grpc_server(self, ip_address, port):
        """Start the gRPC server for the Master Node."""
    
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        mom_pb2_grpc.add_MasterServiceServicer_to_server(self, server)
        mom_pb2_grpc.add_MessageServiceServicer_to_server(self, server)  # Register as MOM instance too
        server.add_insecure_port(f"0.0.0.0:{port}")  # For IPv4
        server.add_insecure_port(f"[::]:{port}")      # For IPv6
        print(f"[MasterNode] gRPC server starting on port {port}...")
        server.start()
    
        # Verify that the server is actually listening on the port
        if not self.verify_server_listening(port):
            print(f"[❌] Failed to start server on port {port}. Port may be in use or blocked.")
            server.stop(0)
            return False
        
        print(f"[✅] Server successfully started on port {port}")
        check_port_externally_accessible(port)
                
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
        
        return True
    
    def verify_server_listening(self, port, max_attempts=5):
        """Verify if the server is listening on the specified port."""
        # Wait for a short time for the server to start up
        time.sleep(1)
        
        for attempt in range(max_attempts):
            try:
                # Check if server is listening on localhost
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    result = s.connect_ex(('localhost', port))
                    if result == 0:
                        print(f"[✅] Server listening on localhost:{port}")
                    else:
                        print(f"[⚠️] Server not listening on localhost:{port}")
                
                # Check if server is listening on external interface
                local_ip = get_local_ip()
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    result = s.connect_ex((local_ip, port))
                    if result == 0:
                        print(f"[✅] Server listening on {local_ip}:{port}")
                        return True
                    else:
                        print(f"[⚠️] Server not listening on {local_ip}:{port}")
                
                time.sleep(1)
            except Exception as e:
                print(f"[⚠️] Error verifying port {port}: {e}")
        
        return False

    def send_message_to_topic(self, topic_name, message):
        """Send a message to a topic via the next available MOM instance, with failover."""
        print(f"[MasterNode] Requesting next available instance for topic '{topic_name}'...")
        
        if not self.mom_instances:
            raise Exception("No MOM instances available")
        
        # Get a list of all instances to try
        node_names = list(self.mom_instances.keys())
        if not node_names:
            raise Exception("No MOM instances available")
        
        # Start with the current instance pointer
        start_idx = self.current_instance
        tried_instances = 0
        offline_instances = []
        
        while tried_instances < len(node_names):
            # Get the next instance in round-robin order
            instance_name = node_names[self.current_instance]
            instance_address = self.mom_instances[instance_name]
            
            # Update round-robin pointer for next call
            self.current_instance = (self.current_instance + 1) % len(node_names)
            tried_instances += 1
            
            try:
                print(f"[MasterNode] Trying to send message to instance {instance_name} at {instance_address}...")
                with grpc.insecure_channel(instance_address, options=[
                    ('grpc.keepalive_time_ms', 5000),
                    ('grpc.keepalive_timeout_ms', 1000),
                    ('grpc.max_reconnect_backoff_ms', 1000),
                    ('grpc.enable_retries', 0),
                    ('grpc.max_receive_message_length', 10 * 1024 * 1024),  # 10MB
                ]) as channel:
                    # Set a shorter timeout for quicker failover
                    stub = mom_pb2_grpc.MessageServiceStub(channel)
                    response = stub.SendMessage(
                        mom_pb2.MessageRequest(topic=topic_name, message=message),
                        timeout=3.0  # 3 second timeout
                    )
                    
                    print(f"[MasterNode] Message sent successfully via {instance_name}")
                    return response
                    
            except Exception as e:
                print(f"[MasterNode] Failed to send message to {instance_name}: {e}")
                offline_instances.append(instance_name)
                # Continue to the next instance
        
        # If we get here, all instances failed
        if offline_instances:
            print(f"[MasterNode] Warning: {len(offline_instances)} instances are unreachable and might need cleanup.")
        
        # Throw exception when all instances have failed
        raise Exception(f"Failed to send message: All {len(node_names)} MOM instances are unreachable")
    
    def health_check_instances(self, auto_remove=False):
        """Periodically check if instances are still alive."""
        print(f"[MasterNode] Running health check on {len(self.mom_instances)} instances...")
        offline_instances = []
        
        for node_name, address in list(self.mom_instances.items()):
            try:
                with grpc.insecure_channel(address, options=[
                    ('grpc.enable_retries', 0),
                    ('grpc.max_reconnect_backoff_ms', 500),
                    ('grpc.keepalive_timeout_ms', 1000),
                ]) as channel:
                    # Use a quick connection check
                    stub = mom_pb2_grpc.MessageServiceStub(channel)
                    # Create a simple ping RPC to check if the instance is alive
                    future = grpc.channel_ready_future(channel)
                    future.result(timeout=2.0)  # 2 second timeout
                    print(f"[MasterNode] ✅ Instance {node_name} at {address} is alive")
            except Exception as e:
                print(f"[MasterNode] ❌ Instance {node_name} at {address} is unreachable: {e}")
                offline_instances.append(node_name)
        
        # Auto-remove unreachable instances if requested
        if offline_instances and auto_remove:
            print(f"[MasterNode] Removing {len(offline_instances)} offline instances...")
            for node_name in offline_instances:
                self.remove_instance(node_name)
            print(f"[MasterNode] Removed {len(offline_instances)} offline instances")
        elif offline_instances:
            print(f"[MasterNode] ⚠️ Found {len(offline_instances)} offline instances")
            print(f"[MasterNode] To clean up, run the 'remove_instance' command for each one")
        
        return len(self.mom_instances) - len(offline_instances)
    
    def start_health_check_thread(self, check_interval=60, auto_remove=False):
        """Start a background thread that periodically checks instance health."""
        import threading
        import time
        
        def health_check_worker():
            while True:
                try:
                    self.health_check_instances(auto_remove=auto_remove)
                except Exception as e:
                    print(f"[MasterNode] Error in health check: {e}")
                time.sleep(check_interval)  # Run every check_interval seconds
        
        thread = threading.Thread(target=health_check_worker, daemon=True)
        thread.start()
        print(f"[MasterNode] Started instance health check thread (interval: {check_interval}s)")

