import json
import os

TOPICS_STATE_FILE = "topics_state.json"

class StateManager:
    def __init__(self, state_file=TOPICS_STATE_FILE):
        self.state_file = state_file
        self.state = self._load_state()

    def _load_state(self):
        """Cargar el estado desde el archivo JSON."""
        if not os.path.exists(self.state_file):
            return {}
        with open(self.state_file, "r") as f:
            return json.load(f)

    def save_state(self):
        """Guardar el estado al archivo JSON."""
        with open(self.state_file, "w") as f:
            json.dump(self.state, f, indent=4)

    def add_topic(self, topic_name, num_partitions):
        """Agregar un tópico al estado y guardarlo."""
        self.state[topic_name] = {"partitions": num_partitions}
        self.save_state()

    def delete_topic(self, topic_name):
        """Eliminar un tópico del estado y guardarlo."""
        if topic_name in self.state:
            del self.state[topic_name]
            self.save_state()

    def restore_state(self, redis_client):
        """Restaurar el estado desde el archivo JSON a Redis."""
        for topic_name, topic_info in self.state.items():
            if not redis_client.exists(topic_name):
                redis_client.sadd("topics", topic_name)
            for partition in range(topic_info["partitions"]):
                partition_key = f"{topic_name}:partition{partition}"
                redis_client.delete(partition_key)  # Limpiar particiones

