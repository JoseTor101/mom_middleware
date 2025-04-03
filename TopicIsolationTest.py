import grpc
import mom_pb2
import mom_pb2_grpc

# Connect to MOM server
channel = grpc.insecure_channel("localhost:50051")
stub = mom_pb2_grpc.MessageServiceStub(channel)

# Send messages to topic "orders"
stub.SendMessage(mom_pb2.MessageRequest(topic="orders", message="Order #1234"))
stub.SendMessage(mom_pb2.MessageRequest(topic="orders", message="Order #5678"))

# Send messages to topic "logs"
stub.SendMessage(mom_pb2.MessageRequest(topic="logs", message="User logged in"))
stub.SendMessage(mom_pb2.MessageRequest(topic="logs", message="Error: Database timeout"))

# Receive messages from "orders"
response1 = stub.ReceiveMessage(mom_pb2.MessageRequest(topic="orders"))
print(f"📦 Orders: {response1.message}")

response2 = stub.ReceiveMessage(mom_pb2.MessageRequest(topic="orders"))
print(f"📦 Orders: {response2.message}")

# Receive messages from "logs"
response3 = stub.ReceiveMessage(mom_pb2.MessageRequest(topic="logs"))
print(f"📜 Logs: {response3.message}")

response4 = stub.ReceiveMessage(mom_pb2.MessageRequest(topic="logs"))
print(f"📜 Logs: {response4.message}")


print("✅ Messages sent successfully!")
