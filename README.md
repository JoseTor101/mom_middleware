# MOM Middleware

MOM Middleware is a distributed **Message-Oriented Middleware** system designed to facilitate communication between clients and multiple MOM instances. It includes features such as topic-based messaging, queue management, and load balancing using a master node.

## Features
- **Master Node**: Manages MOM instances and distributes load using round-robin.
- **MOM Instances**: Handle topics and queues, supporting enqueue and dequeue operations.
- **gRPC Communication**: MOM instances communicate using gRPC for high performance.
- **REST API**: Clients interact with the system via a FastAPI-based REST API.
- **Topic Management**: Create, list, and manage topics with multiple partitions.
- **Message Handling**: Send and receive messages to/from topics.
- **Dynamic Node Registration**: MOM instances can register dynamically with the master node.

## Project Structure
- `client/rest_api.py`: REST API for client interaction.
- `server/node_manager.py`: Master node implementation for managing MOM instances.
- `server/mom_instance.py`: MOM instance implementation for handling topics and queues.
- `server/grpc_generated/`: Auto-generated gRPC code for communication.
- `test/`: Test scripts for validating functionality.

## Requirements
- Python 3.8+
- Redis (for state management)
- gRPC and gRPC tools

## Installation
1. Clone the repository:
```bash
git clone <repository-url>
cd mom_middleware
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

3. Ensure Redis is running:
```bash
redis-server
```

## Usage

Generate the gRPC code:
```bash
cd server
python3 -m grpc_tools.protoc -I. \
--python_out=./grpc_generated \
--grpc_python_out=./grpc_generated \
mom.proto
```


Start the Master Node
Run the master node to manage MOM instances:
```bash
python3 -m server.node_manager
```

Start a MOM Instance
Start a MOM instance and register it with the master node:
```bash
python3 -m server.mom_instance
```

Start the REST API
Run the REST API for client interaction:
```bash
python3 -m uvicorn client.rest_api:app --host 0.0.0.0 --port 8000
```