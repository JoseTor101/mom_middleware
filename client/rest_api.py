from server.round_robin import MasterNode
from server.global_topic import GlobalTopicRegistry
from fastapi import FastAPI
from pydantic import BaseModel
import json

app = FastAPI()
master_node = MasterNode(["localhost:50051", "localhost:50052", "localhost:50053"])

class MessageRequest(BaseModel):
    topic_name: str
    message: str

@app.post("/topic/{topic_name}")
def create_topic(topic_name: str, num_partitions: int = 3):
    master_node.create_topic(topic_name, num_partitions)
    return {"status": "Success", "message": f"Topic {topic_name} created with {num_partitions} partitions"}


@app.post("/list/topics")
def list_topics():
    registry = GlobalTopicRegistry()
    topics = registry.list_topics()
    return {"status": "Success", "topics": topics}

@app.post("/list/instances")
def list_instances():
    instances = master_node.list_instances()
    return {"status": "Success", "instances": instances}

@app.post("/message")
def send_message(request: MessageRequest):
    master_node.send_message(request.topic_name, request.message)
    return {"status": "Success", "message": f"Message sent to topic {request.topic_name}"}
