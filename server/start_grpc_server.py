# server/start_grpc_server.py
import grpc
from concurrent import futures
import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from server.mom_instance import MOMInstance
from server.node_manager import MasterNode
from server.grpc_generated import mom_pb2_grpc

def main():
    # Create a master node for this instance
    master_node = MasterNode()
    
    # Create a MOM instance that will handle GRPC requests
    instance = MOMInstance("grpc_server_instance", master_node)
    
    # Create a GRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mom_pb2_grpc.add_MessageServiceServicer_to_server(instance, server)
    
    # Pick a port to listen on
    port = 50051
    server.add_insecure_port(f'[::]:{port}')
    
    # Register this instance with the master node
    master_node.add_instance("grpc_server_instance", "localhost", port)
    
    # Start the server
    server.start()
    print(f"GRPC server started on port {port}")
    
    try:
        # Keep the server running
        while True:
            time.sleep(86400)  # Sleep for a day
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    main()