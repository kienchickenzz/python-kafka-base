"""
PredictionTopics - Kafka topics cho Prediction domain
"""
from src.shared.enum.KafkaTopic import KafkaTopic, BaseTopics


class PredictionTopics(BaseTopics):
    """Topics cho Prediction domain."""

    # Main topics
    REQUESTED = KafkaTopic("prediction.requested")

    # DLQ topics
    REQUESTED_DLQ = KafkaTopic("prediction.requested.dlq")
