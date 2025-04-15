MOM Middleware

## Usage

Compile the grpc

1. Compile gRPC
python3 -m grpc_tools.protoc -I=server --python_out=server/grpc_generated --grpc_python_out=server/grpc_generated server/mom.proto

2. Activate redis-server
redis-server --daemonize yes

3. Check file sintax
python3 -m server.mom_instance 
python3 -m server.node_manager 
python3 -m client.rest_api

- Generate a secret key

```python
import secrets

# Generate a secure random key
secret_key = secrets.token_urlsafe(32)
print(f"SECRET_KEY={secret_key}")
```

4. Start client
python3 -m uvicorn client.rest_api:app --host 0.0.0.0 --port 8000


5. Signup New User
curl -X POST "http://localhost:8000/signup" -d "username=testuser&password=1234"

6. Login 
curl -X POST "http://localhost:8000/login" -d "username=testuser&password=1234"

This will retrieve an access token(use it)

7. 

curl -X POST "http://localhost:8000/instances/add" \
-H "Authorization: Bearer <Token>" \
-d "instance_address=localhost:50054"


curl -X POST "http://localhost:8000/node/register" \
-H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0dXNlciIsImV4cCI6MTc0NDI1OTQxNH0.KzDxrG2N5QoYeq_r6AiqFOkKatZtfAGdl1YtIOtyboE"

8. List instances

curl -X POST "http://localhost:8000/list/instances" -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0dXNlciIsImV4cCI6MTc0NDE3Nzk0N30.5rTvt_179Vv4YfGgWnkciAm5jTS07K7QmjbZjx0AIbA"


9. Register a MOM Node

curl -X POST "http://localhost:8000/node/register" \
-d '{"node_name": "node-1", "hostname": "127.0.0.1", "port": 50054}' \
-H "Content-Type: application/json"

10. Message

curl -X POST "http://localhost:8000/message" \
-H "Authorization: Bearer <Token>" \
-d '{"topic_name": "orders", "message": "Order #1234"}' \
-H "Content-Type: application/json"

### Registar nueva instancia
python3 -m server.mom_instance --master_node_url http://<master-node-host>:<port>
