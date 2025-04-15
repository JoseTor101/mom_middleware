from server.node_manager import MasterNode
from server.global_topic import GlobalTopicRegistry
from server.mom_instance import MOMInstance
from fastapi import FastAPI, HTTPException, Depends, Form, Request
from pydantic import BaseModel
import jwt
import json
import grpc
from server.auth import (
    hash_password,
    create_access_token,
    authenticate_user,
    fake_users_db,
    SECRET_KEY,
    ALGORITHM,
)
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

app = FastAPI()
master_node = MasterNode()  # Initialize MasterNode without hardcoded instances
mom_instance = MOMInstance(
    instance_name="rest_api_instance",
    master_node_url="http://localhost:8000",  # URL del nodo maestro
    grpc_port=50051  # Puerto gRPC de esta instancia
)
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
        raise HTTPException(status_code=401, detail="Invalid username or password")
    access_token = create_access_token(data={"sub": form_data.username})
    return {"access_token": access_token, "token_type": "bearer"}

def get_current_user(token: str = Depends(oauth2_scheme)):
    """Get the current authenticated user."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None or username not in fake_users_db:
            raise HTTPException(status_code=401, detail="Invalid authentication credentials")
        return username
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")


@app.post("/node/register")
async def register_node(request: Request):
    """Registrar un nodo MOM en el nodo maestro."""
    data = await request.json()  # Usa await para obtener los datos JSON
    node_name = data.get("node_name")
    hostname = data.get("hostname")
    port = data.get("port")

    if not node_name or not hostname or not port:
        raise HTTPException(status_code=400, detail="Invalid node registration data")

    master_node.add_instance(node_name=node_name, hostname=hostname, port=port)
    return {"status": "Success", "message": f"Node {node_name} registered successfully."}

@app.post("/node/remove")
def remove_instance(node_name: str, current_user: str = Depends(get_current_user)):
    """Remove a MOM node from the cluster by its name (authenticated)."""
    master_node.remove_instance(node_name)
    return {"status": "Success", "message": f"Node {node_name} removed from the cluster."}

@app.post("/topic/{topic_name}")
def create_topic(topic_name: str, num_partitions: int = 3, current_user: str = Depends(get_current_user)):
    """Create a new topic (authenticated)."""
    master_node.create_topic(topic_name, num_partitions)
    return {"status": "Success", "message": f"Topic {topic_name} created with {num_partitions} partitions by {current_user}"}

@app.post("/list/topics")
def list_topics():
    registry = GlobalTopicRegistry()
    topics = registry.list_topics()
    return {"status": "Success", "topics": topics}

@app.post("/list/instances")
def list_instances():
    """List all MOM nodes in the cluster."""
    instances = master_node.list_instances()
    return {"status": "Success", "instances": instances}


@app.post("/message")
def send_message(request: MessageRequest, current_user: str = Depends(get_current_user)):
    """Enviar un mensaje a un tópico (autenticado)."""
    try:
        # Obtener la siguiente instancia MOM en round-robin
        instance_name, instance_address = master_node.get_next_instance()
        with grpc.insecure_channel(instance_address) as channel:
            stub = mom_pb2_grpc.MessageServiceStub(channel)
            response = stub.SendMessage(mom_pb2.MessageRequest(topic=request.topic_name, message=request.message))
        return {"status": "Success", "message": f"Message sent to topic {request.topic_name} via {instance_name}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))