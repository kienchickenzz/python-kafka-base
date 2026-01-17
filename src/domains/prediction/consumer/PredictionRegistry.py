"""
PredictionRegistry - Registry cho Prediction domain handlers

Trách nhiệm:
- Định nghĩa handlers cho Prediction domain
- Kế thừa BaseEventRegistry để quản lý polling
"""
from src.shared.base.BaseEventRegistry import BaseEventRegistry
from src.shared.interface.IEventHandler import IEventHandler
from src.domains.prediction.consumer.handlers.PredictionHandler import PredictionHandler


class PredictionRegistry(BaseEventRegistry):
    """
    Registry cho Prediction domain.

    Example:
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        factory = KafkaConsumerFactory(config)
        registry = PredictionRegistry(factory)
        registry.register_all()
    """

    # __init__ inherited từ BaseEventRegistry

    def _create_handlers(self) -> list[IEventHandler]:
        """
        Tạo danh sách handlers cho Prediction domain.

        Returns:
            list[IEventHandler]: Danh sách handlers
        """
        return [
            PredictionHandler(),
        ]
