"""
PredictionHandler - Handler xử lý prediction request từ Kafka

Trách nhiệm:
- Xử lý main business logic (prediction request)
- Xử lý DLQ messages (failed predictions)
"""
from src.consumer.shared.interface.IEventHandler import IEventHandler
from src.consumer.shared.interface.IDLQHandler import IDLQHandler
from src.consumer.prediction.enum.PredictionTopics import PredictionTopics
from src.consumer.prediction.enum.PredictionGroups import PredictionGroups


class PredictionHandler(IEventHandler, IDLQHandler):
    """
    Handler xử lý PredictionRequested event.

    Implements cả IEventHandler và IDLQHandler:
    - handle(): Main business logic
    - handle_dlq(): DLQ handling logic

    Attributes:
        topic: Topic để poll messages
        group: Consumer group ID
        dlq_topic: DLQ topic
        dlq_group: DLQ consumer group ID
    """

    # Main consumer config
    topic = PredictionTopics.REQUESTED
    group = PredictionGroups.WORKER

    # DLQ consumer config
    dlq_topic = PredictionTopics.REQUESTED_DLQ
    dlq_group = PredictionGroups.WORKER_DLQ

    def handle(self, data: dict):
        """
        Xử lý prediction request (main business logic).

        Args:
            data (dict): Event data với keys: request_id, model_name, input_text

        Raises:
            ValueError: Nếu model_name = "fail" (để test DLQ)
            RuntimeError: Nếu input_text chứa "error" (để test DLQ)
        """
        request_id = data.get("request_id")
        model_name = data.get("model_name")
        input_text = data.get("input_text", "")

        print(f"\n[Main] Processing: {request_id}")

        # Test case 1: model_name = "fail" → ValueError
        if model_name == "fail":
            raise ValueError(f"Model '{model_name}' is not supported")

        # Test case 2: input_text chứa "error" → RuntimeError
        if "error" in input_text.lower():
            raise RuntimeError(f"Input text contains forbidden word 'error'")

        # Test case 3: input_text rỗng → ValueError
        if not input_text:
            raise ValueError("Input text cannot be empty")

        # Success case
        print(f"[Main] Model: {model_name}")
        print(f"[Main] Input: {input_text[:50]}..." if len(input_text) > 50 else f"[Main] Input: {input_text}")
        print(f"[Main] Success: {request_id}")

    def handle_dlq(self, data: dict, error_info: dict) -> None:
        """
        Xử lý DLQ message (failed prediction).

        Args:
            data (dict): Original message data
            error_info (dict): Error information
        """
        request_id = data.get("request_id")
        error_message = error_info.get("error_message")
        error_type = error_info.get("error_type")
        retry_count = error_info.get("retry_count", 0)

        print(f"\n[DLQ] ========================================")
        print(f"[DLQ] Failed request: {request_id}")
        print(f"[DLQ] Error type: {error_type}")
        print(f"[DLQ] Error message: {error_message}")
        print(f"[DLQ] Retry count: {retry_count}")
        print(f"[DLQ] Original data: {data}")
        print(f"[DLQ] ========================================")
