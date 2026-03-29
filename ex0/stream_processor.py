from abc import abstractmethod, ABC
from typing import Any, List, Dict, Union, Optional


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        ...

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    def format_output(self, result: str) -> str:
        return result

    def safe_process(self, data: Any) -> str:
        try:
            return self.process(data)
        except TypeError as e:
            return f"{e}"

    def run_batch(
        self,
        dataset: List[Any],
        label: Optional[str] = None
    ) -> Dict[str, Union[str, int]]:
        results: List[str] = []
        for data in dataset:
            results.append(self.safe_process(data))
        return {
            "label": label if label else self.__class__.__name__,
            "count": len(results),
            "last": results[-1] if results else "no data"
        }


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
        split_data = data.split(": ", 1)
        if len(split_data) < 2:
            raise TypeError("Error: Log format must be 'LEVEL: message'")
        return self.format_output(f"{split_data[0]} "
                                  f"level detected: {split_data[1]}")

    def validate(self, data: Any) -> bool:
        return isinstance(data, str) and ": " in data

    def format_output(self, result: str) -> str:
        if "ERROR" in result:
            return f"[ALERT] {result}"
        elif "INFO" in result:
            return f"[INFO] {result}"
        return result


def run_individual_demo(
    numeric: NumericProcessor,
    text: TextProcessor,
    log: LogProcessor
) -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    num_data = [1, 2, 3, 4, 5]
    print("Initializing Numeric Processor...")
    print(f"Processing data: {num_data}")
    print("Validation: Numeric data verified")
    print(f"Output: {numeric.safe_process(num_data)}\n")

    txt_data = "Hello Nexus World"
    print("Initializing Text Processor...")
    print(f'Processing data: "{txt_data}"')
    print("Validation: Text data verified")
    print(f"Output: {text.safe_process(txt_data)}\n")

    log_data = "ERROR: Connection timeout"
    print("Initializing Log Processor...")
    print(f'Processing data: "{log_data}"')
    print("Validation: Log entry verified")
    print(f"Output: {log.safe_process(log_data)}\n")


def run_polymorphic_demo(
    numeric: NumericProcessor,
    text: TextProcessor,
    log: LogProcessor
) -> None:
    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    print(f"Result 1: {numeric.safe_process([2, 2, 2])}")
    print(f"Result 2: {text.safe_process('Mohamed Amine')}")
    print(f"Result 3: {log.safe_process('INFO: System ready')}")
    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    run_individual_demo(numeric, text, log)
    run_polymorphic_demo(numeric, text, log)
