# MOM Middleware

MOM Middleware is a distributed **Message-Oriented Middleware (MOM)** system primarily developed in Python, designed to facilitate asynchronous communication between clients and multiple MOM instances. It includes features such as topic-based messaging, queue management, and load balancing through a master node, aiming to provide a scalable and fault-tolerant solution for service-to-service communication.

## Development Methodology
The project was developed using the agile Scrum methodology, dividing the work into weekly sprints. Each sprint included planning, development, integration, and review. Tasks were managed through GitHub Issues, aligned with specific user stories.

## Team Members

| Name | Institutional Email | Role |
|------|---------------------|------|
| Jose Alejandro Tordecilla | jatordeciz@eafit.edu.co | Scrum Master / Developer |
| Katherin Nathalia Allin Murillo | knallinm@eafit.edu.co | Developer |
| Juan Andrés Montoya Galeano | jamontoya2@eafit.edu.co | Developer |

## Glossary

- **MOM (Message-Oriented Middleware):** Software that enables communication between distributed applications through message exchange.
- **Topic:** A communication channel in the publisher-subscriber pattern.
- **Queue:** A data structure that stores messages in order for processing.
- **gRPC:** A remote communication framework that uses HTTP/2 and Protocol Buffers.
- **Partitioning:** The division of data or workloads to improve scalability.
- **Replication:** The copying of data across multiple nodes to ensure availability and fault tolerance.


## Features

- **Master Node**: Manages MOM instances and distributes load using round-robin.
- **MOM Instances**: Handle topics and queues, supporting enqueue and dequeue operations.
- **gRPC Communication**: MOM instances communicate using gRPC for high performance.
- **REST API**: Clients interact with the system via a FastAPI-based REST API.
- **Topic Management**: Create, list, and manage topics with multiple partitions.
- **Message Handling**: Send and receive messages to/from topics.
- **Dynamic Node Registration**: MOM instances can register dynamically with the master node.
- **Fault Tolerance**: Automatic failover when the master node goes down.
- **Distributed Operation**: Works across different networks and servers.

## Project Structure

```
mom_middleware/
├── client/                  # Client-facing components
│   └── rest_api.py          # REST API for client interaction
├── server/                  # Server-side components
│   ├── master_node.py       # Master node implementation
│   ├── mom_instance.py      # MOM instance implementation
│   ├── join_cluster.py      # Script to join a cluster
│   ├── master_cli.py        # CLI for master node management
│   ├── global_topic.py      # Topic management
│   ├── state_manager.py     # State persistence
│   ├── auth.py              # Authentication
│   ├── mom.proto            # gRPC protocol definition
│   └── grpc_generated/      # Generated gRPC code
├── test/                    # Testing scripts
│   ├── test_rest_api.py     # Python-based API tests
│   ├── test_rest_api.sh     # Bash-based API tests 
│   └── test_topic_isolation.py # Topic isolation tests
├── utils/                   # Utility functions
│   └── utils.py             # Shared utilities
├── __main__.py              # Package entry point
└── topics_state.json        # State persistence file
```
Claro, aquí tienes la traducción al inglés:

---

## Architecture of the MOM Middleware Project
---
### 1. Clients
**Function:** Applications that send and receive messages through the middleware.
**Interaction:** Communicate with the middleware using defined protocols ( HTTP or gRPC).

---

### 2. REST API (Flask)
**Function:** Entry point for clients using HTTP.
**Interaction:** Receives client requests and translates them into operations the middleware can process.

---

### 3. gRPC Server
**Function:** Manages efficient and structured communication between internal system components.
**Interaction:** Facilitates communication between the REST API and the middleware core, as well as between different middleware instances.

---

### 4. Middleware Core (MOM Core)
**Function:** Processes messaging operations such as queue and topic management, message routing, and persistence.
**Interaction:** Receives requests from the REST API or gRPC server and executes the corresponding operations.

---

### 5. Persistent Storage
**Function:** Stores messages and queue information to ensure durability and fault tolerance.
**Interaction:** The middleware core interacts with the storage to save and retrieve data as needed.

---

## Communication Flow

**Message Sending:**
* A client sends an HTTP request to the REST API to publish a message to a specific topic.
* The REST API processes the request and forwards it to the middleware core via the gRPC server.
* The middleware core queues the message in the corresponding topic and stores it persistently.
  
**Message Receiving:**
* A client subscribed to a specific topic makes a request to receive messages.
* The middleware core retrieves the message from storage and delivers it to the client through the REST API.

---

## Technologies

- **Python 3.8+**: Primary programming language
- **gRPC**: High-performance RPC framework for inter-service communication
- **Redis**: For state management, pub/sub, and leader election
- **FastAPI**: REST API framework for client interfaces
- **JWT**: For API authentication and authorization
- **Protocol Buffers**: For service definition and data serialization

## Requirements

- Python 3.8+
- Redis server
- Network connectivity between nodes

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/mom_middleware.git
cd mom_middleware
```

2. Set up a Python virtual environment (recommended):
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Generate the gRPC code:
```bash
cd server
python3 -m grpc_tools.protoc -I. --python_out=./grpc_generated --grpc_python_out=./grpc_generated mom.proto
cd ..
```

5. Ensure Redis is running:
```bash
# Install Redis if not already installed
# Ubuntu: apt install redis-server
# macOS: brew install redis
# Start Redis
redis-server
```

6. Generate secret key for JWT authentication:
```bash
python key.py  # Save the output in .env file
```

7. Create a `.env` file based on `.env.example`:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Usage

### Basic Usage

The system can operate in three different modes:

1. **Master Node**:
   ```bash
   python -m server.master_node_server
   ```

2. **REST API** (connects to existing master node):
   ```bash
   python -m uvicorn client.rest_api:app --host 0.0.0.0 --port 8000
   ```

3. **Worker Node** (joins an existing cluster):
   ```bash
   python -m server.join_cluster --master-url=<master-node-address> --instance-name=<node-name>
   ```

### Distributed Setup

1. **On machine 1** (master server):
   ```bash
   # Start Redis
   redis-server --protected-mode no --bind 0.0.0.0
   
   # Start master node
   python -m server.master_node_server
   ```

2. **On machine 2** (REST API server):
   ```bash
   # Start REST API pointing to Redis on machine 1
   python -m uvicorn client.rest_api:app --host 0.0.0.0 --port 8000 --redis-host=<machine1-ip>
   ```

3. **On machine 3, 4, etc.** (worker nodes):
   ```bash
   # Join the cluster
   python -m server.join_cluster --master-url=<master-public-ip>:<port> --redis-host=<machine1-ip> --instance-name=node-X
   ```  

### REST API Endpoints

| Endpoint | Method | Description | Authentication |
|----------|--------|-------------|----------------|
| `/signup` | POST | Create a user account | None |
| `/login` | POST | Authenticate and get token | None |
| `/node/register` | POST | Register a MOM node | None |
| `/node/remove` | POST | Remove a MOM node | JWT |
| `/topic/{topic_name}` | POST | Create a new topic | JWT |
| `/list/topics` | POST | List all topics | None |
| `/list/instances` | POST | List all nodes | None |
| `/message` | POST | Send a message to a topic | JWT |
| `/message/{topic}/{partition}` | POST | Get message from partition | JWT |
| `/topic/{topic}/info` | POST | Get topic info | JWT |
| `/connect` | GET | Get connection information | None |
| `/topic/{topic}/subscribe` | POST | Subscribe to a topic | JWT |

## Testing

The project includes comprehensive testing scripts to verify functionality:

### Basic Tests

```bash
# Make the test script executable
chmod +x test/test_rest_api.sh

# Run the bash test script
./test/test_rest_api.sh
```

### Python-Based Tests

```bash
# Run the Python API test script
python3 test/test_rest_api.py [optional_api_url]

# Run topic isolation tests
python3 test/test_topic_isolation.py [optional_api_url]
```

### Testing Fault Tolerance

To test the automatic failover capability:

1. Start a master node:
   ```bash
   python3 -m server.master_node_server
   ```

2. Start multiple worker nodes:
   ```bash
   python3 -m server.join_cluster --master-url=<master-url> --instance-name=node-1
   python3 -m server.join_cluster --master-url=<master-url> --instance-name=node-2
   ```

3. Kill the master node process and observe one of the worker nodes automatically taking over as the new master.

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.
