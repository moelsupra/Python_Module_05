from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Union


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.total_processed = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        ...

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch
        return [item for item in data_batch if criteria in str(item)]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "total_processed": self.total_processed
        }

    def safe_process(self, data_batch: List[Any]) -> str:
        try:
            return self.process_batch(data_batch)
        except Exception as e:
            return f"Error: {e}"


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "Environmental Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        if not data_batch:
            return "No data to process!"
        self.total_processed += len(data_batch)
        try:
            temp_values = []
            for item in data_batch:
                key, value = item.split(':')
                if key.strip() == "temp":
                    temp_values.append(float(value))
            if not temp_values:
                return "No temperature data found."
            avg = sum(temp_values) / len(temp_values)
            alerts = []
            for temp in temp_values:
                if temp > 40:
                    alerts.append(f"HIGH:{temp}")
                elif temp < 0:
                    alerts.append(f"LOW:{temp}")
            result = (f"Sensor analysis: {len(data_batch)} readings processed"
                      f", avg temp: {avg}°C")
            if alerts:
                result += f" | ALERTS: {', '.join(alerts)}"
            return result
        except ValueError as e:
            return f"Error parsing data ({e})"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "critical":
            filtered = []
            for item in data_batch:
                try:
                    key, value = item.split(':')
                    if key.strip() == "temp" and float(value) > 40:
                        filtered.append(item)
                except ValueError:
                    pass
            return filtered
        return super().filter_data(data_batch, criteria)


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "Financial Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        if not data_batch:
            return "No data to process!"
        self.total_processed += len(data_batch)
        try:
            buy_values = []
            sell_values = []
            for item in data_batch:
                key, value = item.split(':')
                if key.strip() == "buy":
                    buy_values.append(int(value))
                if key.strip() == "sell":
                    sell_values.append(int(value))
            if not buy_values and not sell_values:
                return "No transaction found."
            flow = sum(buy_values) - sum(sell_values)
            return (f"Transaction analysis: {len(data_batch)} operations, "
                    f"net flow: {'+' if flow > 0 else ''}{flow} units")
        except ValueError as e:
            return f"Error parsing data ({e})"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "large":
            filtered = []
            for item in data_batch:
                try:
                    key, value = item.split(':')
                    if int(value) > 100:
                        filtered.append(item)
                except ValueError:
                    pass
            return filtered
        return super().filter_data(data_batch, criteria)


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "System Events"

    def process_batch(self, data_batch: List[Any]) -> str:
        if not data_batch:
            return "No Event to process!"
        self.total_processed += len(data_batch)
        try:
            errors = [item for item in data_batch
                      if item.strip() == "error"]
            return (f"Event analysis: {len(data_batch)} events, "
                    f"{len(errors)} error detected")
        except ValueError as e:
            return f"Error parsing data ({e})"


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        if not isinstance(stream, DataStream):
            raise TypeError(f"Expected DataStream, got {type(stream)}")
        self.streams.append(stream)

    def process_all(self, batches: List[List[Any]]) -> None:
        labels = [
            ("Sensor data", "readings"),
            ("Transaction data", "operations"),
            ("Event data", "events")
        ]
        for stream, batch, (label, unit) in zip(self.streams, batches, labels):
            stream.safe_process(batch)
            print(f"- {label}: {len(batch)} {unit} processed")


def run_individual_demo() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    sensor = SensorStream("SENSOR_001")
    sensor_data = ["temp:22.5", "humidity:65", "pressure:1013"]
    print("Initializing Sensor Stream...")
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.stream_type}")
    print(f"Processing sensor batch: [{', '.join(sensor_data)}]")
    print(sensor.safe_process(sensor_data))
    print()

    transaction = TransactionStream("TRANS_001")
    trans_data = ["buy:100", "sell:150", "buy:75"]
    print("Initializing Transaction Stream...")
    print(f"Stream ID: {transaction.stream_id}, "
          f"Type: {transaction.stream_type}")
    print(f"Processing transaction batch: [{', '.join(trans_data)}]")
    print(transaction.safe_process(trans_data))
    print()

    event = EventStream("EVENT_001")
    event_data = ["login", "error", "logout"]
    print("Initializing Event Stream...")
    print(f"Stream ID: {event.stream_id}, Type: {event.stream_type}")
    print(f"Processing event batch: [{', '.join(event_data)}]")
    print(event.safe_process(event_data))
    print()


def run_polymorphic_demo() -> None:
    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")

    sensor = SensorStream("SENSOR_001")
    transaction = TransactionStream("TRANS_001")
    event = EventStream("EVENT_001")

    processor = StreamProcessor()
    processor.add_stream(sensor)
    processor.add_stream(transaction)
    processor.add_stream(event)

    sensor_batch = ["temp:20.0", "humidity:70"]
    trans_batch = ["buy:100", "sell:50", "buy:200", "sell:75"]
    event_batch = ["login", "error", "logout"]

    print("Batch 1 Results:")
    processor.process_all([sensor_batch, trans_batch, event_batch])

    print()
    print("Stream filtering active: High-priority data only")
    sensor_alerts = ["temp:50", "temp:22", "temp:55"]
    large = ["buy:100", "sell:150", "buy:75"]
    critical = sensor.filter_data(sensor_alerts, "critical")
    big_trans = transaction.filter_data(large, "large")
    print(f"Filtered results: {len(critical)} critical sensor alerts, "
          f"{len(big_trans)} large transaction")

    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    run_individual_demo()
    run_polymorphic_demo()
