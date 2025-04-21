import argparse
import os
import sys
import time

from server.master_node import MasterNode
from utils.utils import find_free_port, get_local_ip, get_public_ip

def main():
    """Start a master node server."""
    parser = argparse.ArgumentParser(description="Start a MOM middleware master node")
    parser.add_argument(
        "--port",
        type=int,
        default=0,  # Use 0 for dynamic port assignment
        help="Port to use for the master node (default: auto-assigned)"
    )
    parser.add_argument(
        "--redis-host",
        default=os.getenv("REDIS_HOST", "localhost"),
        help="Redis host (default: from REDIS_HOST env var or localhost)"
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=int(os.getenv("REDIS_PORT", 6379)),
        help="Redis port (default: from REDIS_PORT env var or 6379)"
    )
    
    args = parser.parse_args()
    
    # Set environment variables for Redis connection
    os.environ["REDIS_HOST"] = args.redis_host
    os.environ["REDIS_PORT"] = str(args.redis_port)
    
    # Create a MasterNode instance
    print(f"🚀 Creating master node...")
    master_node = MasterNode()
    
    try:
        # Register the master node in Redis
        success, ip, port = master_node.register_master()
        if success:
            print(f"✅ Master node registered successfully")
            print(f"💻 Local address: {ip}:{port}")
            print(f"🌐 Public address: {master_node.public_address}")
            
            # Start the gRPC server (this blocks until interrupted)
            master_node.start_grpc_server(ip, port)
        else:
            print(f"❌ Failed to register master node (already exists)")
            sys.exit(1)
    except KeyboardInterrupt:
        print("👋 Shutting down master node...")
    except Exception as e:
        print(f"❌ Error starting master node: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()