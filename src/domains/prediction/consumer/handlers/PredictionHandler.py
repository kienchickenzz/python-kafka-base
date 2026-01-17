"""
PredictionHandler - Handler xử lý prediction request từ Kafka
"""
from src.shared.interface.IEventHandler import IEventHandler
from src.domains.prediction.enum.PredictionTopics import PredictionTopics
from src.domains.prediction.enum.PredictionGroups import PredictionGroups


class PredictionHandler(IEventHandler):
    """
    Handler xử lý PredictionRequested event.

    Attributes:
        topic: Topic để poll messages
        group: Consumer group ID
    """

    topic = PredictionTopics.REQUESTED
    group = PredictionGroups.WORKER

    def handle(self, data: dict):
        """
        Xử lý prediction request.

        Args:
            data (dict): Event data với keys: request_id, model_name, input_text

        Returns:
            dict: Kết quả prediction với keys: request_id, result, confidence
        """
        request_id = data.get("request_id")

        # Mock prediction
        return {
            "request_id": request_id,
            "result": "positive",
            "confidence": 0.85,
        }
