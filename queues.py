import threading

class MessageQueueService:
    def __init__(self):
        """Diccionario para almacenar las colas de mensajes."""
        self.queues = {}
        self.lock = threading.Lock()

    def create_queue(self, queue_name: str):
        """Crea una nueva cola si no existe."""
        with self.lock:
            if queue_name not in self.queues:
                self.queues[queue_name] = []
                print(f"Cola '{queue_name}' creada.")
            else:
                print(f"La cola '{queue_name}' ya existe.")

    def delete_queue(self, queue_name: str):
        """Elimina una cola si existe."""
        with self.lock:
            if queue_name in self.queues:
                del self.queues[queue_name]
                print(f"Cola '{queue_name}' eliminada.")
            else:
                print(f"La cola '{queue_name}' no existe.")

    def send_message(self, queue_name: str, message: str):
        """Envía un mensaje a la cola especificada."""
        with self.lock:
            if queue_name in self.queues:
                self.queues[queue_name].append(message)
                print(f"Mensaje enviado a '{queue_name}': {message}")
            else:
                print(f"La cola '{queue_name}' no existe.")

    def receive_message(self, queue_name: str):
        """Recibe un mensaje de la cola (FIFO)."""
        with self.lock:
            if queue_name in self.queues and self.queues[queue_name]:
                message = self.queues[queue_name].pop(0)
                print(f"Mensaje recibido de '{queue_name}': {message}")
                return message
            else:
                print(f"No hay mensajes en la cola '{queue_name}'.")
                return None

if __name__ == "__main__":
    service = MessageQueueService()

    while True:
        print("\nOpciones:")
        print("1. Crear cola")
        print("2. Eliminar cola")
        print("3. Enviar mensaje")
        print("4. Recibir mensaje")
        print("5. Salir")

        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            queue_name = input("Nombre de la cola: ")
            service.create_queue(queue_name)

        elif opcion == "2":
            queue_name = input("Nombre de la cola: ")
            service.delete_queue(queue_name)

        elif opcion == "3":
            queue_name = input("Nombre de la cola: ")
            message = input("Mensaje: ")
            service.send_message(queue_name, message)

        elif opcion == "4":
            queue_name = input("Nombre de la cola: ")
            service.receive_message(queue_name)

        elif opcion == "5":
            print("Saliendo...")
            break

        else:
            print("Opción inválida, intenta de nuevo.")
