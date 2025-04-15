import socket
s = socket.socket()
s.connect(("127.0.0.1", 45401))  # Should fail if nothing is running
