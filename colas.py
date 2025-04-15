import threading
import json
import os

class FaultTolerantMessageQueueService:
    def __init__(self, backup_file='backup_queues.json'):
        self.queues = {}
        self.lock = threading.Lock()
        self.backup_file = backup_file
        self._load_backup()

    def _load_backup(self):
        """Carga el estado de las colas desde el archivo de respaldo."""
        if os.path.exists(self.backup_file):
            try:
                with open(self.backup_file, 'r') as f:
                    self.queues = json.load(f)
                    print("Backup cargado correctamente.")
            except Exception as e:
                print(f"Error al cargar el backup: {e}")

    def _save_backup(self):
        """Guarda el estado actual de las colas en el archivo de respaldo."""
        try:
            with open(self.backup_file, 'w') as f:
                json.dump(self.queues, f)
        except Exception as e:
            print(f"Error al guardar backup: {e}")

    def create_queue(self, queue_name: str):
        with self.lock:
            if queue_name not in self.queues:
                self.queues[queue_name] = []
                self._save_backup()
                print(f"Cola '{queue_name}' creada.")
            else:
                print(f"La cola '{queue_name}' ya existe.")

    def delete_queue(self, queue_name: str):
        with self.lock:
            if queue_name in self.queues:
                del self.queues[queue_name]
                self._save_backup()
                print(f"Cola '{queue_name}' eliminada.")
            else:
                print(f"La cola '{queue_name}' no existe.")

    def send_message(self, queue_name: str, message: str):
        with self.lock:
            if queue_name in self.queues:
                self.queues[queue_name].append(message)
                self._save_backup()
                print(f"Mensaje enviado a '{queue_name}': {message}")
            else:
                print(f"La cola '{queue_name}' no existe.")

    def receive_message(self, queue_name: str):
        with self.lock:
            if queue_name in self.queues and self.queues[queue_name]:
                message = self.queues[queue_name].pop(0)
                self._save_backup()
                print(f"Mensaje recibido de '{queue_name}': {message}")
                return message
            else:
                print(f"No hay mensajes en la cola '{queue_name}'.")
                return None


if __name__ == "__main__":
    service = FaultTolerantMessageQueueService()

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
