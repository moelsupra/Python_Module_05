from abc import abstractmethod, ABC
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        ...

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise TypeError("Error: Type of Data is not valid to process")
        count = len(data)
        sum_num = sum(data)
        return self.format_output(f"Processed {count} numeric values, "
                                  f"sum={sum_num}, avg={sum_num/count}")

    def validate(self, data: Any) -> bool:
        if not isinstance(data, list) or len(data) == 0:
            return False
        return all(isinstance(x, (int, float)) for x in data)


class TextProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise TypeError("Error: Invalid type of data expected string")
        count = len(data)
        count_word = len(data.split())
        return self.format_output(f"Processed text: {count} characters, "
                                  f"{count_word} words")

    def validate(self, data: Any) -> bool:
        return isinstance(data, str) and len(data) > 0


class LogProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise TypeError("Error: Invalid type of data")
        splited_data = data.split(": ", 1)
        return self.format_output(f"{splited_data[0]} "
                                  f"level detected: {splited_data[1]}")

    def validate(self, data: Any) -> bool:
        return isinstance(data, str) and ":" in data

    def format_output(self, result: str) -> str:
        if "ERROR" in result:
            return super().format_output(f"[ALERT] {result}")
        elif "INFO" in result:
            return super().format_output(f"[INFO] {result}")
        return super().format_output(result)


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    numeric = NumericProcessor()
    data = [1, 2, 3, 4, 5]
    print("Initializing Numeric Processor...")
    if numeric.validate(data):
        print(f"Processing data: {data}")
        print("Validation: Numeric data verified")
    print(numeric.process(data), "\n")

    text = TextProcessor()
    data = "Hello Nexus World"
    print("Initializing Text Processor...")
    if text.validate(data):
        print(f"Processing data: \"{data}\"")
        print("Validation: Text data verified")
    print(text.process(data), "\n")

    log = LogProcessor()
    data = "ERROR: Connection timeout"
    print("Initializing Log Processor...")
    if log.validate(data):
        print(f"Processing data: \"{data}\"")
        print("Validation: Log entry verified")
    print(log.process(data), "\n")

    print("=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...")

    print(f"Result 1: {numeric.process([2, 2, 2])}")
    print(f"Result 2: {text.process("Mohamed Amine")}")
    print(f"Result 3: {log.process("INFO: System ready")}")

    print("\nFoundation systems online. Nexus ready for advanced streams.")
