"""
PredictionGroups - Consumer groups cho Prediction domain
"""
from src.shared.enum.ConsumerGroup import ConsumerGroup, BaseGroups


class PredictionGroups(BaseGroups):
    """Consumer groups cho Prediction domain."""

    # Main groups
    WORKER = ConsumerGroup("prediction-worker")
    ANALYTICS = ConsumerGroup("prediction-analytics")

    # DLQ groups
    WORKER_DLQ = ConsumerGroup("prediction-worker-dlq")
    ANALYTICS_DLQ = ConsumerGroup("prediction-analytics-dlq")
