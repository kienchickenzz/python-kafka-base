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
import uuid
from typing import Any

from src.shared.base.BaseEventPublisher import BaseEventPublisher
from src.shared.interface.IEventPublisher import IEventPublisher
from src.domains.prediction.enum.PredictionTopics import PredictionTopics


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
        # Validate model_name
        model_name = payload.get('model_name')
        if not model_name:
            raise ValueError("model_name is required in payload")
        if not isinstance(model_name, str):
            raise ValueError("model_name must be a string")

        # Validate input_text
        input_text = payload.get('input_text')
        if not input_text:
            raise ValueError("input_text is required in payload")
        if not isinstance(input_text, str):
            raise ValueError("input_text must be a string")
        if len(input_text) > 10000:
            raise ValueError("input_text must be <= 10000 characters")

        # Generate request_id nếu chưa có
        request_id = payload.get('request_id')
        if not request_id:
            request_id = f"pred-{uuid.uuid4().hex[:8]}"

        # Build event data
        event_data = {
            'request_id': request_id,
            'model_name': model_name,
            'input_text': input_text,
        }

        # Publish
        partition_key = key or request_id
        self.publish(self.topic, event_data, partition_key)
