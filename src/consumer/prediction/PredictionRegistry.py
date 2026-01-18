"""
PredictionRegistry - Registry cho Prediction domain handlers

Trách nhiệm:
- Định nghĩa handlers cho Prediction domain
- Kế thừa BaseEventRegistry để quản lý polling (main + DLQ)
"""
from src.consumer.shared.base.BaseEventRegistry import BaseEventRegistry
from src.consumer.shared.interface.IEventHandler import IEventHandler
from src.consumer.prediction.handler.PredictionHandler import PredictionHandler


class PredictionRegistry(BaseEventRegistry):
    """
    Registry cho Prediction domain.

    Mỗi handler sẽ có 2 consumers: main và DLQ.

    Example:
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        consumer_factory = KafkaConsumerFactory(config)
        dlq_publisher = DeadLetterPublisher(producer, serializer)

        registry = PredictionRegistry(consumer_factory, dlq_publisher)
        registry.register_all()
    """

    # __init__ inherited từ BaseEventRegistry

    def _create_handlers(self) -> list[IEventHandler]:
        """
        Tạo danh sách handlers cho Prediction domain.

        Mỗi handler PHẢI implement cả IEventHandler và IDLQHandler.

        Returns:
            list[IEventHandler]: Danh sách handlers
        """
        return [
            PredictionHandler(),
        ]
