import json
from pathlib import Path
from rocksdict import Rdict


class StateManager:
    def __init__(self, db_path="week-3/state/worker-state"):
        Path(db_path).mkdir(parents=True, exist_ok=True)

        self.db = Rdict(db_path)

        print(f"RocksDB state store opened: {db_path}")

    def save_state(self, truck_id, state):
        self.db[truck_id] = json.dumps(state)

    def get_state(self, truck_id):
        value = self.db.get(truck_id)

        if value is None:
            return None

        return json.loads(value)

    def get_all_state(self):
        state = {}

        for key, value in self.db.items():
            state[key] = json.loads(value)

        return state

    def close(self):
        self.db.close()
        print("RocksDB state store closed.")