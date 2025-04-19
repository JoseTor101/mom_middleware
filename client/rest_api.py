import os
import sys
# Add the parent directory to the path so Python can find the 'server' module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import jwt
from fastapi import Depends, FastAPI, Form, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel

from server.auth import (ALGORITHM, SECRET_KEY, authenticate_user,
                         create_access_token, fake_users_db, hash_password)
from server.global_topic import GlobalTopicRegistry
from server.node_manager import MasterNode

app = FastAPI()

# Initialize the MasterNode at the start of the program
master_node = MasterNode()
try:
    master_node.register_master()
    print("✅ Master Node initialized and registered successfully.")
except Exception as e:
    print(f"❌ Failed to initialize Master Node: {e}")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")


class MessageRequest(BaseModel):
    topic_name: str
    message: str


@app.post("/signup")
def signup(username: str = Form(...), password: str = Form(...)):
    """Signup a new user."""
    if username in fake_users_db:
        raise HTTPException(status_code=400, detail="Username already exists")
    fake_users_db[username] = {"hashed_password": hash_password(password)}
    return {"status": "Success", "message": f"User {username} created"}


@app.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """Login a user and return a JWT token."""
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Invalid username or password")
    access_token = create_access_token(data={"sub": form_data.username})
    return {
        "access_token": access_token, "token_type": "bearer"}


def get_current_user(token: str = Depends(oauth2_scheme)):
    """Get the current authenticated user."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None or username not in fake_users_db:
            raise HTTPException(
                status_code=401, detail="Invalid authentication credentials"
            )
        return username
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=401, detail="Invalid authentication credentials"
        )


@app.post("/node/register")
def register_node(ip: str = Form(None)):
    """Register a MOM node in the cluster."""
    global master_node
    if master_node is None:
        raise HTTPException(status_code=500,
                            detail="Master Node is not initialized.")
    master_node.add_instance(ip_address=ip)
    return {"status": "Success", "message": "Node registered successfully."}


@app.post("/node/remove")
def remove_instance(
        node_name: str,
        current_user: str = Depends(get_current_user)):
    """Remove a MOM node from the cluster by its name (authenticated)."""
    global master_node
    if master_node is None:
        raise HTTPException(status_code=500,
                            detail="Master Node is not initialized.")
    master_node.remove_instance(node_name)
    return {
        "status": "Success",
        "message": f"Node {node_name} removed from the cluster.",
    }


@app.post("/topic/{topic_name}")
def create_topic(
    topic_name: str,
    num_partitions: int = 3,
    current_user: str = Depends(get_current_user),
):
    """Create a new topic (authenticated)."""
    global master_node
    if master_node is None:
        raise HTTPException(status_code=500,
                            detail="Master Node is not initialized.")
    try:
        master_node.create_topic(topic_name, num_partitions)
        return {
            "status": "Success",
            "message": f"Topic {topic_name} created with {num_partitions} partitions by {current_user}",
        }
    except Exception as e:
        print(f"Error creating topic: {e}")
        raise HTTPException(status_code=500,
                            detail=f"Error creating topic: {str(e)}")


@app.post("/list/topics")
def list_topics():
    registry = GlobalTopicRegistry()
    topics = registry.list_topics()
    return {"status": "Success", "topics": topics}


@app.post("/list/instances")
def list_instances():
    """List all MOM nodes in the cluster."""
    global master_node
    if master_node is None:
        raise HTTPException(status_code=500,
                            detail="Master Node is not initialized.")
    instances = master_node.list_instances()
    return {"status": "Success", "instances": instances}


@app.post("/message")
def send_message(
    request: MessageRequest, current_user: str = Depends(get_current_user)
):
    """Send a message to a topic (authenticated)."""
    global master_node
    if master_node is None:
        raise HTTPException(status_code=500,
                            detail="Master Node is not initialized.")
    response = master_node.send_message_to_topic(
        request.topic_name, request.message)
    return {
        "status": "Success",
        "message": f"Message sent to topic {request.topic_name} via {response.status}",
    }


@app.post("/topic/{topic_name}/info")
def get_topic_info(
        topic_name: str,
        current_user: str = Depends(get_current_user)):
    """Get information about a topic and its partitions."""
    registry = GlobalTopicRegistry()
    partition_count = registry.get_partition_count(topic_name)
    partition_stats = registry.get_partition_stats(topic_name)

    return {
        "status": "Success",
        "topic_name": topic_name,
        "partition_count": partition_count,
        "partition_stats": partition_stats,
    }


@app.post("/message/{topic_name}/{partition_id}")
def get_message_from_partition(
        topic_name: str,
        partition_id: int,
        current_user: str = Depends(get_current_user)):
    """Get a message from a specific partition."""
    registry = GlobalTopicRegistry()
    message = registry.get_message_from_partition(topic_name, partition_id)

    if message:
        return {
            "status": "Success",
            "topic_name": topic_name,
            "partition_id": partition_id,
            "message": message,
        }
    else:
        return {
            "status": "Empty",
            "topic_name": topic_name,
            "partition_id": partition_id,
            "message": "No messages available in this partition",
        }
    

@app.post("/topic/{topic_name}/subscribe")
def subscribe_to_topic(topic_name: str, current_user: str = Depends(get_current_user)):
    """Get all messages from a topic (authenticated)."""
    registry = GlobalTopicRegistry()
    messages = registry.get_all_messages_from_topic(topic_name)
    
    return {
        "status": "Success",
        "topic_name": topic_name,
        "message_count": len(messages),
        "messages": messages,
    }


def main():
    import sys

    current_user = None
    current_user_token = None

    while True:
        print("\n====== MOM CLI Menu ======")
        print("1. Signup")
        print("2. Login")
        print("3. Register Node")
        print("4. Remove Node")
        print("5. Create Topic")
        print("6. List Topics")
        print("7. List Nodes")
        print("8. Send Message")
        print("9. Get Message from Partition")
        print("10. Subscribe to Topic")  # New option added here
        print("0. Exit")

        choice = input("Choose an option: ")
        # Signup
        if choice == "1":
            username = input("Choose username: ")
            password = input("Choose password: ")
            try:
                response = signup(username=username, password=password)
                print(f"✅ {response['message']}")
            except Exception as e:
                print(f"❌ Signup failed: {e}")
        # Login
        elif choice == "2":
            username = input("Username: ")
            password = input("Password: ")
            try:
                form_data = OAuth2PasswordRequestForm(
                    username=username,
                    password=password,
                    scope="",
                    grant_type="",
                    client_id=None,
                    client_secret=None,
                )
                response = login(form_data)
                current_user_token = response["access_token"]
                current_user = get_current_user(current_user_token)
                print(f"🔐 Logged in as {current_user}")
            except Exception as e:
                print(f"❌ Login failed: {e}")

        # Add node
        elif choice == "3":
            ip = input("Enter IP address (leave blank for local): ").strip()
            ip = ip if ip else None
            master_node.add_instance(ip_address=ip)
            print(f"✅ Node registered successfully.")
        # Remove node
        elif choice == "4":
            if not current_user_token:
                print("🔒 Please login first.")
                continue
            name = input("Enter node name to remove: ")
            master_node.remove_instance(name)
            print(f"❌ Node '{name}' removed.")
        # Create topic
        elif choice == "5":
            if not current_user_token:
                print("🔒 Please login first.")
                continue
            topic = input("Enter topic name: ")
            partitions = int(input("Enter number of partitions: "))
            master_node.create_topic(topic, partitions)
            print(f"✅ Topic '{topic}' created with {partitions} partitions.")
        # List topics
        elif choice == "6":
            registry = GlobalTopicRegistry()
            topics = registry.list_topics()
            print("📋 Topics:")
            for t in topics:
                print(f" - {t}")
        # List nodes
        elif choice == "7":
            instances = master_node.list_instances()
            print("🖥️  Registered Nodes:")
            for i in instances:
                print(f" - {i}")
        # Send message
        elif choice == "8":
            if not current_user_token:
                print("🔒 Please login first.")
                continue
            topic = input("Enter topic name: ")
            msg = input("Enter message: ")
            try:
                response = master_node.send_message_to_topic(topic, msg)
                print(f"📤 Message sent via: {response.status}")
            except Exception as e:
                print(f"❌ Failed to send message: {e}")
        # Get message from partition
        elif choice == "9":
            if not current_user_token:
                print("🔒 Please login first.")
                continue
            topic = input("Enter topic name: ")
            pid = int(input("Enter partition ID: "))
            registry = GlobalTopicRegistry()
            msg = registry.get_message_from_partition(topic, pid)
            if msg:
                print(f"📬 Message from {topic}[{pid}]: {msg}")
            else:
                print("📭 No messages in that partition.")
        # Subscribe to topic
        elif choice == "10":
            if not current_user_token:
                print("🔒 Please login first.")
                continue
            topic = input("Enter topic name to subscribe to: ")
            registry = GlobalTopicRegistry()
            
            print(f"\n📬 Subscription to topic '{topic}' active. Showing all messages:")
            messages = registry.get_all_messages_from_topic(topic)
            
            if messages:
                print(f"📚 {len(messages)} messages found in topic '{topic}':")
                for i, msg in enumerate(messages, 1):
                    print(f"  {i}. {msg}")
                
                # Ask if user wants to keep listening for new messages
                keep_listening = input("\nDo you want to keep listening for new messages? (y/n): ").lower()
                if keep_listening == 'y':
                    print(f"📡 Listening for new messages on topic '{topic}'... (Press Ctrl+C to stop)")
                    try:
                        # Keep checking for new messages
                        last_count = len(messages)
                        while True:
                            import time
                            time.sleep(2)  # Check every 2 seconds
                            new_messages = registry.get_all_messages_from_topic(topic)
                            if len(new_messages) > last_count:
                                # Only display new messages
                                for i, msg in enumerate(new_messages[last_count:], last_count+1):
                                    print(f"  {i}. {msg}")
                                last_count = len(new_messages)
                    except KeyboardInterrupt:
                        print("\n📴 Subscription stopped.")
            else:
                print(f"📭 No messages found in topic '{topic}'.")

        elif choice == "0":
            print("👋 Exiting...")
            sys.exit(0)

        else:
            print("❌ Invalid option. Try again.")


if __name__ == "__main__":
    main()