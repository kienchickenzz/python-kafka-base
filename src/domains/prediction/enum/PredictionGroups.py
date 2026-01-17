"""
PredictionGroups - Consumer groups cho Prediction domain
"""
from src.shared.enum.ConsumerGroup import ConsumerGroup, BaseGroups


class PredictionGroups(BaseGroups):
    """Consumer groups cho Prediction domain."""

    WORKER = ConsumerGroup("prediction-worker")
    ANALYTICS = ConsumerGroup("prediction-analytics")
