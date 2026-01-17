"""
KafkaConsumerFactory - Factory để tạo KafkaConsumer cho handlers

Trách nhiệm:
- Tạo KafkaConsumer instance cho từng handler
- Encapsulate config và serialization setup
"""
from kafka import KafkaConsumer

from src.infrastructure.KafkaConfig import KafkaConfig
from src.shared.base.JsonSerializer import JsonSerializer
from src.shared.interface.IEventHandler import IEventHandler


class KafkaConsumerFactory:
    """
    Factory để tạo KafkaConsumer instances.

    Example:
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        factory = KafkaConsumerFactory(config)

        handler = PredictionHandler()
        consumer = factory.create(handler)
    """

    def __init__(self, config: KafkaConfig) -> None:
        """
        Initialize factory với config.

        Args:
            config (KafkaConfig): Kafka configuration
        """
        self._config = config
        # Truyền serializer từ bên ngoài 
        self._serializer = JsonSerializer()

    def create(self, handler: IEventHandler) -> KafkaConsumer:
        """
        Tạo KafkaConsumer cho handler.

        Lấy topic và group từ handler class attributes.

        Args:
            handler (IEventHandler): Handler với topic và group

        Returns:
            KafkaConsumer: Consumer instance
        """
        return KafkaConsumer(
            handler.topic.value,
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=handler.group.value,
            auto_offset_reset=self._config.auto_offset_reset,
            enable_auto_commit=self._config.enable_auto_commit,
            value_deserializer=lambda m: self._serializer.deserialize_raw(m),
        )
