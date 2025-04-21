#!/usr/bin/env python3

import argparse
import os
import sys
import redis

def check_master_node():
    """Check if a master node is running and display its information."""
    # Connect to Redis
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    
    # Check if master node is registered
    master_address = r.get("master_node")
    if not master_address:
        print("❌ No master node is currently registered.")
        return False
        
    # Get more information
    public_address = r.get("master_node_public")
    heartbeat = r.get("master_node_heartbeat")
    is_alive = heartbeat is not None
    
    print("\n===== MASTER NODE STATUS =====")
    print(f"📡 Local address: {master_address}")
    if public_address:
        print(f"🌐 Public address: {public_address}")
    print(f"💓 Heartbeat: {'✅ Active' if is_alive else '❌ Missing'}")
    
    # Check registered instances
    from server.state_manager import StateManager
    state_manager = StateManager()
    state = state_manager._load_state()
    instances = state.get("mom_instances", {})
    
    print(f"\n🖥️  Registered instances ({len(instances)}):")
    for name, addr in instances.items():
        print(f"  - {name}: {addr}")
    
    print("\n===== CONNECTION COMMAND =====")
    print(f"python -m mom_middleware join --master-url={public_address or master_address} --instance-name=my-node\n")
    
    return True

def clear_master_node():
    """Clear master node registration from Redis."""
    # Connect to Redis
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    
    # Delete all master node keys
    r.delete("master_node")
    r.delete("master_node_public")
    r.delete("master_node_port")
    r.delete("master_node_heartbeat")
    
    print("✅ Master node registration cleared.")
    return True

def main():
    """Master node management CLI."""
    parser = argparse.ArgumentParser(description="MOM Master Node Management")
    parser.add_argument(
        "action",
        choices=["status", "clear"],
        help="Action to perform: status (check master status), clear (clear master registration)"
    )
    
    args = parser.parse_args()
    
    if args.action == "status":
        check_master_node()
    elif args.action == "clear":
        clear_master_node()

if __name__ == "__main__":
    main()