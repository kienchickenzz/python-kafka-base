"""
PredictionHandler - Handler xử lý prediction request từ Kafka

Trách nhiệm:
- Xử lý main business logic (prediction request)
- Xử lý DLQ messages (failed predictions)
"""
from src.shared.interface.IEventHandler import IEventHandler
from src.shared.interface.IDLQHandler import IDLQHandler
from src.domains.prediction.enum.PredictionTopics import PredictionTopics
from src.domains.prediction.enum.PredictionGroups import PredictionGroups


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
        """
        request_id = data.get("request_id")
        model_name = data.get("model_name")
        input_text = data.get("input_text", "")

        print(f"[Main] Processing prediction request: {request_id}")
        print(f"[Main] Model: {model_name}")
        print(f"[Main] Input: {input_text[:50]}..." if len(input_text) > 50 else f"[Main] Input: {input_text}")

        # Mock prediction logic
        # Trong thực tế sẽ gọi ML model ở đây

    def handle_dlq(self, data: dict, error_info: dict) -> None:
        """
        Xử lý DLQ message (failed prediction).

        Args:
            data (dict): Original message data
            error_info (dict): Error information với keys:
                - error_message, error_type, timestamp, original_topic, handler_name, retry_count
        """
        request_id = data.get("request_id")
        error_message = error_info.get("error_message")
        retry_count = error_info.get("retry_count", 0)

        print(f"[DLQ] Processing failed prediction: {request_id}")
        print(f"[DLQ] Error: {error_message}")
        print(f"[DLQ] Retry count: {retry_count}")

        # DLQ handling logic:
        # 1. Log error for monitoring
        # 2. Alert if critical
        # 3. Optionally retry with backoff
        # 4. Store for manual review
