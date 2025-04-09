MOM Middleware

## Usage

Compile the grpc

1. Compile gRPC
python3 -m grpc_tools.protoc -I=server --python_out=server/grpc_generated --grpc_python_out=server/grpc_generated server/mom.proto

2. Activate redis-server
redis-server --daemonize yes

3. Check file sintax
python3 -m server.mom_instance 
python3 -m server.round_robin 
python3 -m client.rest_api

4. Start client
python3 -m uvicorn client.rest_api:app --host 0.0.0.0 --port 8000

5. Test connection
curl -X POST "http://localhost:8000/topic/test"
curl -X POST "http://localhost:8000/topic/list"