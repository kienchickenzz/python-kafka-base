"""
PredictionTopics - Kafka topics cho Prediction domain
"""
from src.shared.enum.KafkaTopic import KafkaTopic, BaseTopics


class PredictionTopics(BaseTopics):
    """Topics cho Prediction domain."""

    REQUESTED = KafkaTopic("prediction.requested")
    COMPLETED = KafkaTopic("prediction.completed")
    FAILED = KafkaTopic("prediction.failed")
