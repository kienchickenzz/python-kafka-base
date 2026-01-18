"""
PredictionPublisher - Publisher cho Prediction domain

Trách nhiệm:
- Validate input cho prediction events
- Build event data với domain-specific logic
- Publish prediction events vào Kafka topic

Inheritance:
- BaseEventPublisher: cung cấp publish() generic implementation
- IEventPublisher: định nghĩa create_event() contract
"""
from typing import Any

from src.producer.shared.base.BaseEventPublisher import BaseEventPublisher
from src.producer.shared.interface.IEventPublisher import IEventPublisher
from src.producer.prediction.enum.PredictionTopics import PredictionTopics


class PredictionPublisher(BaseEventPublisher, IEventPublisher):
    """
    Publisher cho Prediction domain.

    Multiple Inheritance:
    - BaseEventPublisher: inherit __init__(), publish(), flush(), close()
    - IEventPublisher: implement create_event() contract

    Chịu trách nhiệm:
    - Implement create_event() với prediction-specific validation
    - Build event data cho prediction domain
    - Sử dụng publish() từ BaseEventPublisher

    Example:
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        factory = KafkaProducerFactory(config)
        producer = factory.create()
        serializer = JsonSerializer()

        publisher = PredictionPublisher(producer, serializer)
        publisher.create_event({
            "model_name": "sentiment",
            "input_text": "Hello world",
        })
        publisher.flush()
        publisher.close()
    """

    topic = PredictionTopics.REQUESTED

    # __init__ inherited từ BaseEventPublisher
    # publish(), flush(), close() inherited từ BaseEventPublisher

    def create_event(
        self,
        payload: dict[str, Any],
        key: str | None = None,
    ) -> None:
        """
        Tạo và publish prediction event với business logic.

        Args:
            payload (dict[str, Any]): Prediction event payload với fields:
                - model_name (str): Tên model để predict (required)
                - input_text (str): Text input (required)
                - request_id (str): Custom request ID (optional)
            key (str | None): Partition key (optional, default = request_id)

        Raises:
            ValueError: Nếu payload không hợp lệ
        """
        model_name = payload.get('model_name')
        input_text = payload.get('input_text')
        request_id = payload.get('request_id')

        # Build event data
        event_data = {
            'request_id': request_id,
            'model_name': model_name,
            'input_text': input_text,
        }

        # Publish
        partition_key = key or request_id
        self.publish(self.topic, event_data, partition_key)
