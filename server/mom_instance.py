import os
import socket
import sys
import time
from concurrent import futures
from .state_manager import StateManager
import dotenv
import grpc
import requests

from utils.utils import get_local_ip, get_public_ip
from server.global_topic import GlobalTopicRegistry

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
from .grpc_generated import mom_pb2, mom_pb2_grpc

class MOMInstance(mom_pb2_grpc.MessageServiceServicer):
    def __init__(self, instance_name, master_node_url=None, grpc_port=50051):
        self.instance_name = instance_name
        self.master_node_url = master_node_url  # This can be the public address for remote machines
        self.grpc_port = grpc_port
        self.registry = GlobalTopicRegistry()
        
        # For connecting to Redis, use environment variables or defaults
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        try:
            import redis
            self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        except Exception as e:
            print(f"Failed to connect to Redis: {e}")

    def get_master_address(self):
        """Get the master node address from the provided URL or from Redis."""
        if self.master_node_url:
            # If a direct URL was provided, use it
            if ":" in self.master_node_url:
                # Extract hostname and port
                hostname, port = self.master_node_url.split(":")
                
                # Check if connecting to own public IP
                public_ip = get_public_ip()
                if hostname == public_ip:
                    print(f"[{self.instance_name}] Detected connection to own public IP, using local network instead")
                    local_ip = get_local_ip()
                    return f"{local_ip}:{port}"
                
                return self.master_node_url
        try:
            # Try to get the public address first (for remote machines)
            public_address = self.redis.get("master_node_public")
            if public_address:
                return public_address
                
            # Fall back to internal address
            internal_address = self.redis.get("master_node")
            if internal_address:
                return internal_address
        except Exception as e:
            print(f"Error retrieving master address from Redis: {e}")
        
        raise Exception("Could not determine master node address. Please provide master_node_url.")

    def register_with_master_node(self):
        """Register this MOM instance with the Master Node via gRPC."""
        master_address = self.get_master_address()
        print(f"🔄 [{self.instance_name}] Connecting to master at {master_address}...")
        hostname = get_local_ip()
        print(f"[{self.instance_name}] Using IP: {hostname}")
        
        with grpc.insecure_channel(master_address) as channel:
            stub = mom_pb2_grpc.MasterServiceStub(channel)
            response = stub.RegisterMOMInstance(
                mom_pb2.MOMInstanceRegistrationRequest(
                    node_name=self.instance_name,
                    hostname=hostname,
                    port=self.grpc_port))
    
        if response.status == "Success":
            print(f"[{self.instance_name}] Successfully registered with MasterNode.")
            
            # Synchronize topic information with Redis
            self._sync_topics_from_state_file()
            
            return True
        else:
            print(f"[{self.instance_name}] Failed to register with MasterNode: {response.message}")
            return False
    
    def _sync_topics_from_state_file(self):
        """Load topics from state file and add them to Redis."""
        try:
            state_manager = StateManager()
            state = state_manager._load_state()
            
            # Process each key that isn't 'mom_instances'
            for topic_name, topic_info in state.items():
                if topic_name != 'mom_instances' and isinstance(topic_info, dict) and 'partitions' in topic_info:
                    partitions = topic_info['partitions']
                    print(f"[{self.instance_name}] Syncing topic {topic_name} with {partitions} partitions")
                    self.registry.create_topic(topic_name, partitions)
        except Exception as e:
            print(f"[{self.instance_name}] Error syncing topics: {e}")

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
        return mom_pb2.MessageResponse(status="Success", message="Message enqueued")

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

    def replicate_partition(self, topic_name, partition, target_instance):
        partition_key = f"{topic_name}:partition{partition}"
        messages = self.registry.redis.lrange(partition_key, 0, -1)
        with grpc.insecure_channel(target_instance) as channel:
            stub = mom_pb2_grpc.MessageServiceStub(channel)
            for message in messages:
                stub.SendMessage(
                    mom_pb2.MessageRequest(
                        topic=topic_name,
                        message=message.decode()))

    def start_server(self):
        """Start the gRPC server for this MOM instance."""
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        mom_pb2_grpc.add_MessageServiceServicer_to_server(self, server)
        server.add_insecure_port(f"[::]:{self.grpc_port}")
        
        # Start the server BEFORE registering with master
        server.start()
        print(f"[{self.instance_name}] gRPC server started on port {self.grpc_port}")
        
        # Now register with the master node
        self.register_with_master_node()
        print(f"[{self.instance_name}] Ready to process requests.")
    
        # Make server non-blocking
        import threading
        thread = threading.Thread(target=server.wait_for_termination)
        thread.daemon = True
        thread.start()
        
        # Store the server object so it doesn't get garbage collected
        self.server = server
        return server
