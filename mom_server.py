import grpc
from concurrent import futures
import mom_pb2
import mom_pb2_grpc
import redis
import os
from fastapi import FastAPI

# Configuración de Redis Cluster
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Servidor FastAPI para API REST
app = FastAPI()

@app.post("/topic/{topic_name}")
def create_topic(topic_name: str):
    redis_client.sadd("topics", topic_name)
    return {"status": "Success", "message": f"Topic {topic_name} created"}

@app.get("/topics")
def list_topics():
    topics = redis_client.smembers("topics")
    return {"topics": list(topics)}

class MessageService(mom_pb2_grpc.MessageServiceServicer):
    def SendMessage(self, request, context):
        topic_key = f"topic:{request.topic}"
        print(f"Storing message '{request.message}' in Redis under {topic_key}")
        redis_client.rpush(topic_key, request.message)  # Agrega el mensaje a Redis
        return mom_pb2.MessageResponse(status="Success", message="Message received")

    def ReceiveMessage(self, request, context):
        topic_key = f"topic:{request.topic}"
        message = redis_client.lpop(topic_key)  # Obtiene el mensaje más antiguo
        print(f"Retrieving message from {topic_key}: {message}")
        return mom_pb2.MessageResponse(status="Success", message=message or "No messages available")

def serve_grpc():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mom_pb2_grpc.add_MessageServiceServicer_to_server(MessageService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    import threading
    threading.Thread(target=serve_grpc, daemon=True).start()
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
