from .grpc_generated import mom_pb2, mom_pb2_grpc
import os
import socket
import sys
import time
from concurrent import futures

import dotenv
import grpc
import requests

from server.global_topic import GlobalTopicRegistry

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))


class MOMInstance(mom_pb2_grpc.MessageServiceServicer):
    def __init__(self, instance_name, master_node_url, grpc_port=50051):
        self.instance_name = instance_name
        self.master_node_url = master_node_url
        self.grpc_port = grpc_port
        self.registry = GlobalTopicRegistry()

    def get_master_instance(self):
        """Get the master node instance address."""
        master_address = self.master_node.get_master()
        if master_address:
            # Aquí puedes conectarte con el master usando su dirección
            print(f"Conectando al nodo master en {master_address}")
            return master_address
        else:
            print("No se pudo encontrar un nodo master.")
            return None

    def register_with_master_node(self):
        """Register this MOM instance with the Master Node via gRPC."""
        master_address = self.get_master_address()  # Retrieve the master node address
        hostname = socket.gethostbyname(socket.gethostname())
        with grpc.insecure_channel(master_address) as channel:
            stub = mom_pb2_grpc.MasterServiceStub(channel)
            response = stub.RegisterMOMInstance(
                mom_pb2.MOMInstanceRegistrationRequest(
                    node_name=self.instance_name,
                    hostname=hostname,
                    port=self.grpc_port))

        if response.status == "Success":
            print(f"[{self.instance_name}] Successfully registered with MasterNode.")
        else:
            print(
                f"[{self.instance_name}] Failed to register with MasterNode: {response.message}"
            )

    def start(self):
        """Start the MOM instance and register with the Master Node."""
        self.register_with_master_node()
        print(f"[{self.instance_name}] Ready to process requests.")

    def SendMessage(self, request, context):
        """Send a message to the specified topic."""
        print(
            f"[{self.instance_name}] Received message for topic '{request.topic}': {request.message}"
        )
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

    def start_server(self):
        """Start the gRPC server for this MOM instance."""
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        mom_pb2_grpc.add_MessageServiceServicer_to_server(self, server)
        server.add_insecure_port(f"[::]:{self.grpc_port}")
        server.start()
        print(f"[{self.instance_name}] gRPC server started on port {self.grpc_port}")

        # Make server non-blocking
        import threading

        thread = threading.Thread(target=server.wait_for_termination)
        thread.daemon = True
        thread.start()
