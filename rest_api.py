from wsgiref.simple_server import make_server
import json
import grpc
import mom_pb2
import mom_pb2_grpc

def rest_app(environ, start_response):
    path = environ["PATH_INFO"]
    if path == "/send_message":
        request_body = environ["wsgi.input"].read(int(environ.get("CONTENT_LENGTH", 0)))
        data = json.loads(request_body)

        # Connect to MOM gRPC Server
        channel = grpc.insecure_channel("localhost:50051")
        stub = mom_pb2_grpc.MessageServiceStub(channel)
        response = stub.SendMessage(mom_pb2.MessageRequest(topic=data["topic"], message=data["message"]))

        start_response("200 OK", [("Content-Type", "application/json")])
        return [json.dumps({"status": response.status, "message": response.message}).encode("utf-8")]

    start_response("404 Not Found", [("Content-Type", "text/plain")])
    return [b"Not Found"]

if __name__ == "__main__":
    server = make_server("0.0.0.0", 8000, rest_app)
    print("REST API running on port 8000...")
    server.serve_forever()
