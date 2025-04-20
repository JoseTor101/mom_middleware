#!/usr/bin/env python3

import argparse
import os
import socket
import sys
import time

from server.mom_instance import MOMInstance
from server.master_node import MasterNode
from utils.utils import find_free_port

def get_hostname():
    """Get the machine's hostname."""
    return socket.gethostname()


def main():
    """Join a remote MOM middleware cluster or create a new master node if connection fails."""
    parser = argparse.ArgumentParser(description="Join a MOM middleware cluster")
    parser.add_argument(
        "--master-url",
        required=True,
        help="URL of the master node (e.g., 192.168.1.100:50051)"
    )
    parser.add_argument(
        "--instance-name",
        default=f"node-{get_hostname()}",
        help="Name for this node instance (default: node-hostname)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,  # Use 0 for dynamic port assignment
        help="Port to use for this MOM instance (default: auto-assigned)"
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
    parser.add_argument(
        "--create-master-if-fails",
        action="store_true",
        default=True,  # Default to True to create master node if connection fails
        help="Create a new master node if connection to specified master fails"
    )
    
    args = parser.parse_args()
    
    # Set environment variables for Redis connection
    os.environ["REDIS_HOST"] = args.redis_host
    os.environ["REDIS_PORT"] = str(args.redis_port)
    
    # Create and start a MOM instance
    print(f"🌐 Connecting to master node at: {args.master_url}")
    print(f"📝 Instance name: {args.instance_name}")
    
    port = args.port
    if port == 0:
        # Find an available port dynamically
        port = find_free_port()
        print(f"🔌 Using dynamically assigned port: {port}")
    
    try:
        mom_instance = MOMInstance(
            instance_name=args.instance_name,
            master_node_url=args.master_url,
            grpc_port=port
        )
        
        # Try to register with the master node
        try:
            mom_instance.start_server()
            print(f"✅ Successfully joined the MOM middleware cluster")
            print(f"⏳ Waiting for messages...")
        except Exception as e:
            if args.create_master_if_fails:
                print(f"❌ Failed to connect to master node: {e}")
      
        # Keep the script running to receive messages
        try:
            while True:
                time.sleep(3600)  # Sleep for an hour and wake up to check if still running
        except KeyboardInterrupt:
            print("👋 Shutting down node...")
    
    except Exception as e:
        print(f"❌ Failed to start node: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()