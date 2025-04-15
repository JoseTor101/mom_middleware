import json
import os

TOPICS_STATE_FILE = "topics_state.json"

class StateManager:
    def __init__(self, state_file=TOPICS_STATE_FILE):
        self.state_file = state_file
        self.state = self._load_state()

    def _load_state(self):
        if not os.path.exists(self.state_file):
            return {}
        with open(self.state_file, "r") as f:
            return json.load(f)

    def save_state(self):
        with open(self.state_file, "w") as f:
            json.dump(self.state, f, indent=4)

    def add_topic(self, topic_name, num_partitions):
        self.state[topic_name] = {"partitions": num_partitions}
        self.save_state()

    def delete_topic(self, topic_name):
        if topic_name in self.state:
            del self.state[topic_name]
            self.save_state()
