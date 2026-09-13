import json
from pathlib import Path
from datetime import datetime, timedelta
from rocksdict import Rdict


class StateManager:

    def __init__(self, db_path="week-3/state/worker-1"):
        Path(db_path).mkdir(parents=True, exist_ok=True)

        self.db = Rdict(db_path)

        print(f"RocksDB state store opened: {db_path}")

    def save_reading(self, truck_id, temperature, timestamp):

        state = self.get_state(truck_id)

        if state is None:
            state = {
                "truck_id": truck_id,
                "readings": []
            }

        state["readings"].append({
            "temperature": temperature,
            "timestamp": timestamp
        })

        # Keep only readings from the last 5 minutes
        current_time = datetime.fromisoformat(timestamp)
        cutoff_time = current_time - timedelta(minutes=5)

        state["readings"] = [
            reading
            for reading in state["readings"]
            if datetime.fromisoformat(
                reading["timestamp"]
            ) >= cutoff_time
        ]

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

    def get_rolling_average(self, truck_id):

        state = self.get_state(truck_id)

        if state is None or not state["readings"]:
            return None

        temperatures = [
            reading["temperature"]
            for reading in state["readings"]
        ]

        return sum(temperatures) / len(temperatures)

    def restore_state(self, truck_id, state):

        self.db[truck_id] = json.dumps(state)

        print(
            f"State restored for {truck_id} "
            f"from Kafka changelog ✓"
        )

    def close(self):

        self.db.close()

        print("RocksDB state store closed.")