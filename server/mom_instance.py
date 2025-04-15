import grpc
from concurrent import futures
import time 
import sys
import os
import requests
import socket
from server.global_topic import GlobalTopicRegistry

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
from .grpc_generated import mom_pb2, mom_pb2_grpc

class MOMInstance(mom_pb2_grpc.MessageServiceServicer):
    def __init__(self, instance_name, master_node_url, grpc_port=50051):
        self.instance_name = instance_name
        self.master_node_url = master_node_url
        self.grpc_port = grpc_port

    def register_with_master_node(self):
        """Registrar esta instancia MOM en el nodo maestro."""
        hostname = socket.gethostbyname(socket.gethostname())
        response = requests.post(
            f"{self.master_node_url}/node/register",
            json={
                "node_name": self.instance_name,
                "hostname": hostname,
                "port": self.grpc_port
            }
        )
        if response.status_code == 200:
            print(f"[{self.instance_name}] Successfully registered with MasterNode.")
        else:
            print(f"[{self.instance_name}] Failed to register with MasterNode: {response.text}")
    
    def start_grpc_server(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        mom_pb2_grpc.add_MessageServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(f"[::]:{self.grpc_port}")
        print(f"[{self.instance_name}] gRPC server starting on port {self.grpc_port}...")
        self.server.start()
        self.register_with_master_node()
        try:
            self.server.wait_for_termination()
        except KeyboardInterrupt:
            print(f"[{self.instance_name}] Shutting down gRPC server...")
            self.server.stop(0)

    def send_message_to_topic(self, topic_name, message):
        print(f"[{self.instance_name}] Sending message to topic '{topic_name}': {message}")
        instance_name, instance_address = self.master_node.get_next_instance()
        with grpc.insecure_channel(instance_address) as channel:
            stub = mom_pb2_grpc.MessageServiceStub(channel)
            response = stub.SendMessage(mom_pb2.MessageRequest(topic=topic_name, message=message))
        return response

    def SendMessage(self, request, context):
        print(f"[{self.instance_name}] Received message for topic '{request.topic}': {request.message}")

        registry = GlobalTopicRegistry()
        registry.enqueue_message(request.topic, request.message)

        self.master_node.log_message(request.topic, request.message, action="ENQUEUE")
        self.registry.enqueue_message(request.topic, request.message)
        return mom_pb2.MessageResponse(status="Success", message="Message enqueued")

    def ReceiveMessage(self, request, context):
        print(f"[{self.instance_name}] Processing message for topic '{request.topic}'")
        
        # Get a message from any partition (round-robin)
        registry = GlobalTopicRegistry()
        partition_count = registry.get_partition_count(request.topic)
        message = None
        
        if partition_count > 0:
            # Try each partition in round-robin fashion
            for i in range(partition_count):
                message = registry.dequeue_message(request.topic, i)
                if message:
                    break
        
        if message:
            return mom_pb2.MessageResponse(status="Success", message=message)
        else:
            return mom_pb2.MessageResponse(status="Empty", message="No messages available")
        
    def replicate_partition(self, topic_name, partition, target_instance):
        partition_key = f"{topic_name}:partition{partition}"
        messages = self.registry.redis.lrange(partition_key, 0, -1)
        with grpc.insecure_channel(target_instance) as channel:
            stub = mom_pb2_grpc.MessageServiceStub(channel)
            for message in messages:
                stub.SendMessage(mom_pb2.MessageRequest(topic=topic_name, message=message.decode()))
