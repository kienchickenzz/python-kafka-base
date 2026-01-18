"""
PredictionTopics - Kafka topics cho Prediction domain
"""
from src.producer.shared.enum.KafkaTopic import KafkaTopic, BaseTopics


class PredictionTopics(BaseTopics):
    """Topics cho Prediction domain."""

    # Main topics
    REQUESTED = KafkaTopic("prediction.requested")
