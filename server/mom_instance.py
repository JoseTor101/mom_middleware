import grpc
from concurrent import futures
import time
import os
import sys
import requests
import socket
import dotenv

from server.global_topic import GlobalTopicRegistry

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
from .grpc_generated import mom_pb2, mom_pb2_grpc

MASTER_NODE_HOST = os.getenv("MASTER_NODE_HOST", "localhost")
MASTER_NODE_PORT = os.getenv("MASTER_NODE_PORT", 60051)

class MOMInstance(mom_pb2_grpc.MessageServiceServicer):
    def __init__(self, instance_name, master_node_url, grpc_port=50051):
        self.instance_name = instance_name
        self.master_node_url = master_node_url
        self.grpc_port = grpc_port
        self.registry = GlobalTopicRegistry()

    """def register_with_master_node(self):
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
            print(f"[{self.instance_name}] Failed to register with MasterNode: {response.text}")"""
            
    def get_master_instance(self):
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
            response = stub.RegisterMOMInstance(mom_pb2.MOMInstanceRegistrationRequest(
                node_name=self.instance_name, hostname=hostname, port=self.grpc_port))
        
        if response.status == "Success":
            print(f"[{self.instance_name}] Successfully registered with MasterNode.")
        else:
            print(f"[{self.instance_name}] Failed to register with MasterNode: {response.message}")

    def start(self):
        """Conectarse al MasterNode y manejar las solicitudes desde allí."""
        self.register_with_master_node()

        # Las instancias MOM ya no inician un servidor gRPC local.
        # En lugar de eso, se conectan al MasterNode para enviar y recibir mensajes.
        print(f"[{self.instance_name}] Ready to process requests.")
        # Aquí puedes agregar más lógica si es necesario para la interacción con el MasterNode

    def SendMessage(self, request, context):
        print(f"[{self.instance_name}] Received message for topic '{request.topic}': {request.message}")
        self.registry.enqueue_message(request.topic, request.message)
        return mom_pb2.MessageResponse(status="Success", message="Message enqueued")

    def ReceiveMessage(self, request, context):
        print(f"[{self.instance_name}] Processing message for topic '{request.topic}'")
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
            return mom_pb2.MessageResponse(status="Empty", message="No messages available")

    def send_message_to_topic(self, topic_name, message):
        print(f"[{self.instance_name}] Requesting next available instance from MasterNode via gRPC...")
        print(f"Connecting to MasterNode at {MASTER_NODE_HOST}:{MASTER_NODE_PORT}")
        with grpc.insecure_channel(f"{MASTER_NODE_HOST}:{MASTER_NODE_PORT}") as channel:
            stub = mom_pb2_grpc.MasterServiceStub(channel)
            response = stub.GetNextInstance(mom_pb2.Empty())
            instance_address = response.address
    
        print(f"[{self.instance_name}] Sending message to {instance_address}")
        with grpc.insecure_channel(instance_address) as channel:
            stub = mom_pb2_grpc.MessageServiceStub(channel)
            response = stub.SendMessage(mom_pb2.MessageRequest(topic=topic_name, message=message))
        return response

    def replicate_partition(self, topic_name, partition, target_instance):
        partition_key = f"{topic_name}:partition{partition}"
        messages = self.registry.redis.lrange(partition_key, 0, -1)
        with grpc.insecure_channel(target_instance) as channel:
            stub = mom_pb2_grpc.MessageServiceStub(channel)
            for message in messages:
                stub.SendMessage(mom_pb2.MessageRequest(topic=topic_name, message=message.decode()))
