The Colas.py file implements a basic in-memory message queuing system as part of Message-Oriented Middleware (MOM). Its main purpose is to enable reliable point-to-point communication, where messages are stored in a queue until the consumer receives them.
Uses a basic disk persistence strategy to ensure queues are not lost if the server shuts down unexpectedly or restarts.
Automatically saves queue state (names and messages) to a backup_queues.json file.
Uses locking with threading.Lock() to prevent multiple threads from corrupting data.

## Main Functionality:
- Create new message queues.
- Delete existing queues.
- List all active queues.
- Send messages to a specific queue.
- Receive messages from a queue.

## Running the Code
You can test the queuing system by directly running the Colas.py file. Run the following command:
` python Colas.py `
