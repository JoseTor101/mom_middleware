1. Set Up Environment

pip install -r requirements.txt

2. Generate Protocol Buffer Code

cd server
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. mom.proto
cd ..

3. Start Redis Server

redis-server --daemonize yes

4. Generate Secret Key

python key.py

5. Start the gRPC(MOM) Server

python server/start_grpc_server.py

6. Start the REST API
In a different terminal:

python -m uvicorn client.rest_api:app --host 0.0.0.0 --port 8000

7. Register the gRPC Server with the REST API

curl -X POST "http://localhost:8000/node/register" \
     -d "node_name=grpc_server_instance&hostname=127.0.0.1&port=50051" \
     -H "Content-Type: application/x-www-form-urlencoded"

8. Create a User Account

curl -X POST "http://localhost:8000/signup" \
     -d "username=testuser&password=testpass" \
     -H "Content-Type: application/x-www-form-urlencoded"

9. Login to Get an Authentication Token

curl -X POST "http://localhost:8000/login" \
     -d "username=testuser&password=testpass" \
     -H "Content-Type: application/x-www-form-urlencoded"

This will return an access token. Save it.

10. Create a Topic

curl -X POST "http://localhost:8000/topic/test" \
     -H "Authorization: Bearer YOUR_KEY_HERE"

11. Send a Message to the Topic

curl -X POST "http://localhost:8000/message" \
     -d '{"topic_name": "test", "message": "Hello, MOM!"}' \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer YOUR_KEY_HERE"

12. Verify
List registered Nodes:

curl -X POST "http://localhost:8000/list/instances"

List available topics:

curl -X POST "http://localhost:8000/list/topics"