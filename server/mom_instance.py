import os
import socket
import sys
import time
from concurrent import futures
from .state_manager import StateManager
import dotenv
import grpc
import requests
import random

from utils.utils import get_local_ip, get_public_ip, find_free_port
from server.global_topic import GlobalTopicRegistry

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
from .grpc_generated import mom_pb2, mom_pb2_grpc

class MOMInstance(mom_pb2_grpc.MessageServiceServicer):
    def __init__(self, instance_name, master_node_url=None, grpc_port=50051):
        self.instance_name = instance_name
        self.master_node_url = master_node_url  # This can be the public address for remote machines
        self.grpc_port = grpc_port
        self.registry = GlobalTopicRegistry()
        self.promoting_to_master = False
        self.election_priority = random.random()  # Random priority for leader election
        
        # For connecting to Redis, use environment variables or defaults
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        try:
            import redis
            self.redis = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
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
        
        try:
            with grpc.insecure_channel(master_address, options=[
                ('grpc.enable_retries', 0),
                ('grpc.max_reconnect_backoff_ms', 500),
            ]) as channel:
                future = grpc.channel_ready_future(channel)
                future.result(timeout=5.0)  # 5 second timeout to check connection
                
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
        except Exception as e:
            print(f"[{self.instance_name}] Failed to connect to master node: {e}")
            
            if "create_master_if_fails" in sys.argv:
                print(f"[{self.instance_name}] Attempting to become master node...")
                self.participate_in_leader_election()
            else:
                print(f"[{self.instance_name}] Use --create-master-if-fails to create a new master node")
            
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

    def check_master_status(self):
        """Check if the master node is alive."""
        try:
            # First check if master node record exists at all
            master_address = self.redis.get("master_node")
            if not master_address:
                print(f"[{self.instance_name}] ⚠️ Master node address not found in Redis")
                return False
            
            # Then check heartbeat
            heartbeat = self.redis.get("master_node_heartbeat")
            if not heartbeat:
                print(f"[{self.instance_name}] ⚠️ Master node heartbeat not found, checking connection...")
                
                # Double-check by trying to connect
                try:
                    with grpc.insecure_channel(master_address, options=[
                        ('grpc.enable_retries', 0),
                        ('grpc.max_reconnect_backoff_ms', 500),
                        ('grpc.keepalive_timeout_ms', 1000),
                    ]) as channel:
                        # Set a very short timeout to quickly detect if server is down
                        future = grpc.channel_ready_future(channel)
                        future.result(timeout=1.0)  # 1 second timeout
                        
                        # If we get here, the server is actually responsive
                        print(f"[{self.instance_name}] Master node is actually responsive despite missing heartbeat")
                        return True
                except Exception as e:
                    print(f"[{self.instance_name}] ❌ Confirmed master node is down: {e}")
                    return False
            
            # If we have both master record and heartbeat, the master is alive
            return True
        except Exception as e:
            print(f"[{self.instance_name}] Error checking master status: {e}")
            return False

    def participate_in_leader_election(self):
        """Attempt to become the new master node."""
        # Use a Redis lock to ensure only one instance becomes master
        import time
        import uuid
        
        # Generate a unique ID for this election attempt
        election_attempt_id = str(uuid.uuid4())
        
        # Use our election priority for an ordered election
        priority_key = f"election:priority:{self.instance_name}"
        try:
            # Set our priority in Redis (lower number = higher priority)
            self.redis.set(priority_key, str(self.election_priority), ex=30)  # 30-second expiry
            
            # Wait for a random time between 1-3 seconds to avoid all nodes trying at once
            delay = 1.0 + random.random() * 2.0
            print(f"[{self.instance_name}] Waiting {delay:.2f}s before attempting leader election")
            time.sleep(delay)
            
            # First verify that master node key is actually missing from Redis
            if self.redis.exists("master_node"):
                # Double check if it's really alive by trying to connect
                master_address = self.redis.get("master_node")
                if not master_address:
                    # Master node key exists but is empty, proceed with election
                    print(f"[{self.instance_name}] Master node key exists but is empty")
                else:
                    try:
                        # Try to connect with a very short timeout
                        with grpc.insecure_channel(master_address, options=[
                            ('grpc.enable_retries', 0),
                            ('grpc.max_reconnect_backoff_ms', 500),
                            ('grpc.keepalive_timeout_ms', 1000),
                        ]) as channel:
                            future = grpc.channel_ready_future(channel)
                            future.result(timeout=1.0)  # 1 second timeout
                            
                            # If we get here, the master is actually alive
                            print(f"[{self.instance_name}] Master node is actually responsive, aborting election")
                            return False
                    except Exception as e:
                        # Failed to connect, proceed with election despite key existing
                        print(f"[{self.instance_name}] Master node key exists but connection failed: {e}")
            
            # Try to acquire the election lock
            election_lock = self.redis.lock("master_node_election", timeout=30)
            got_lock = False
            
            try:
                got_lock = election_lock.acquire(blocking=False)
                if got_lock:
                    print(f"[{self.instance_name}] Acquired election lock, checking if master still down...")
                    
                    # Double-check master node is actually gone from Redis
                    if not self.redis.exists("master_node") or not self.check_master_status():
                        print(f"[{self.instance_name}] 🏆 Confirmed master is down, promoting self to master")
                        
                        # Force clear all master registrations to avoid conflicts
                        self.redis.delete("master_node")
                        self.redis.delete("master_node_public")
                        self.redis.delete("master_node_port")
                        self.redis.delete("master_node_heartbeat")
                        
                        try:
                            # Create a MasterNode instance and register it
                            from server.master_node import MasterNode
                            new_master = MasterNode()
                            
                            # Set the instance name to maintain identity
                            new_master.instance_name = f"{self.instance_name}-master"
                            
                            # Register as the master node
                            success, ip, port = new_master.register_master()
                            
                            if success:
                                print(f"[{self.instance_name}] ✅ Successfully promoted to master node")
                                
                                # Transfer node's state to master node
                                self.transfer_state_to_master(new_master)
                                
                                # Start the gRPC server for the master node (non-daemon thread)
                                import threading
                                thread = threading.Thread(
                                    target=new_master.start_grpc_server,
                                    args=(ip, port),
                                    daemon=False  # Use non-daemon thread so it keeps running
                                )
                                thread.start()
                                
                                # Set a flag that we're now the master
                                self.redis.set(f"node:{self.instance_name}:is_master", "true")
                                print(f"[{self.instance_name}] 👑 Now operating as master node")
                                
                                return True
                            else:
                                print(f"[{self.instance_name}] ❌ Failed to register as master node")
                        except Exception as e:
                            print(f"[{self.instance_name}] Error during master promotion: {e}")
                    else:
                        print(f"[{self.instance_name}] Master node is back online, aborting election")
                else:
                    print(f"[{self.instance_name}] Another node is already performing leader election")
            finally:
                # Always release the lock if we acquired it
                if got_lock:
                    try:
                        election_lock.release()
                        print(f"[{self.instance_name}] Released election lock")
                    except Exception as e:
                        print(f"[{self.instance_name}] Error releasing election lock: {e}")
        except Exception as e:
            print(f"[{self.instance_name}] Error during leader election: {e}")
        
        return False

    def transfer_state_to_master(self, new_master):
        """Transfer this node's state to the new master node."""
        # First, make sure to read the latest state
        state_manager = StateManager()
        state = state_manager._load_state()
        
        # Transfer MOM instance registrations
        for node_name, address in state.get("mom_instances", {}).items():
            new_master.mom_instances[node_name] = address
        
        # Save state to file 
        new_master._save_state()
        
        # Set in Redis that we're the elected master
        self.redis.set("elected_master", self.instance_name)
        self.redis.set("elected_master_time", time.time())
        
        print(f"[{self.instance_name}] ✅ Successfully transferred state to new master")

    def start_master_monitoring_thread(self):
        """Start a background thread that monitors master health and triggers failover if needed."""
        import threading
        
        def monitoring_worker():
            # Initial delay to allow system to stabilize
            print(f"[{self.instance_name}] Starting master node monitoring (waiting for 15s)")
            time.sleep(15)
            
            failures_count = 0
            max_failures = 2  # Reduced to make failover faster
            is_master_node = False
            
            while not is_master_node:
                try:
                    # Skip checking if we're already promoting
                    if hasattr(self, 'promoting_to_master') and self.promoting_to_master:
                        time.sleep(5)
                        continue
                    
                    # First, check if master node record exists in Redis at all
                    master_exists = self.redis.exists("master_node")
                    if not master_exists:
                        print(f"\n[{self.instance_name}] 🔴 MASTER NODE EXPLICITLY UNREGISTERED: Record missing from Redis")
                        print(f"[{self.instance_name}] 🚨 INITIATING IMMEDIATE FAILOVER")
                        
                        # Immediate failover when master is explicitly unregistered
                        self.promoting_to_master = True
                        success = self.participate_in_leader_election()
                        
                        if success:
                            print(f"\n[{self.instance_name}] 👑👑👑 NOW OPERATING AS MASTER NODE 👑👑👑")
                            is_master_node = True
                            break  # Exit the monitoring loop - we're now the master
                        else:
                            # Reset flag but continue monitoring
                            self.promoting_to_master = False
                            failures_count = 0
                            time.sleep(3)  # Wait a bit before next check
                            continue
                        
                    # Otherwise check if master is alive via heartbeat/connection
                    if not self.check_master_status():
                        failures_count += 1
                        print(f"\n[{self.instance_name}] 🚨 Master node appears to be down ({failures_count}/{max_failures})")
                        
                        if failures_count >= max_failures:
                            print(f"\n[{self.instance_name}] 🚨 MASTER NODE DOWN! Confirmed after {failures_count} checks")
                            print(f"[{self.instance_name}] 🚨 INITIATING LEADER ELECTION...\n")
                            
                            # Slight delay to prevent all nodes from trying simultaneously
                            time.sleep(random.uniform(0.5, 2))
                            
                            # Try to become the new master
                            self.promoting_to_master = True
                            success = self.participate_in_leader_election()
                            
                            if success:
                                print(f"\n[{self.instance_name}] 👑👑👑 NOW OPERATING AS MASTER NODE 👑👑👑")
                                is_master_node = True
                                break  # Exit the monitoring loop - we're now the master
                            
                            # Reset flag and counter regardless of result
                            self.promoting_to_master = False
                            failures_count = 0
                    else:
                        if failures_count > 0:
                            print(f"[{self.instance_name}] Master node is responsive again, resetting failure count")
                        failures_count = 0  # Reset counter if master is responsive
                except Exception as e:
                    print(f"[{self.instance_name}] Error in master monitoring: {e}")
                    failures_count = 0  # Reset on errors
                
                # Check every 5 seconds (reduced from 10 for faster detection)
                time.sleep(5)
            
            # If we get here, we've become the master node
            print(f"[{self.instance_name}] Master monitoring thread exiting - now operating as master")
        
        # Initialize flag and start monitoring thread
        self.promoting_to_master = False
        thread = threading.Thread(target=monitoring_worker, daemon=True)
        thread.daemon = True  # Make thread exit when main program exits
        thread.start()
        print(f"[{self.instance_name}] Started master node monitoring thread")

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
        
        # Start monitoring master node health for potential failover
        self.start_master_monitoring_thread()

        # Make server non-blocking
        import threading
        thread = threading.Thread(target=server.wait_for_termination)
        thread.daemon = True
        thread.start()
        
        # Store the server object so it doesn't get garbage collected
        self.server = server
        return server
