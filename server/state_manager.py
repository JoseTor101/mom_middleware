import json
import os

class StateManager:
    def __init__(self, filepath="master_state.json"):
        self.filepath = filepath

    def save_state(self, data: dict):
        with open(self.filepath, "w") as f:
            json.dump(data, f, indent=4)

    def load_state(self) -> dict:
        if os.path.exists(self.filepath):
            with open(self.filepath, "r") as f:
                return json.load(f)
        return {}
