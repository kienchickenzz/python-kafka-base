"""
run_consumer - Entry point để chạy Kafka consumer

Trách nhiệm:
- Khởi tạo các dependencies (config, factories, publishers)
- Tạo và chạy registry
"""
from src.consumer.infrastructure.KafkaConfig import KafkaConfig
from src.consumer.infrastructure.KafkaConsumerFactory import KafkaConsumerFactory
from src.consumer.infrastructure.KafkaProducerFactory import KafkaProducerFactory
from src.consumer.infrastructure.DeadLetterPublisher import DeadLetterPublisher
from src.shared.base.JsonSerializer import JsonSerializer
from src.consumer.prediction.PredictionRegistry import PredictionRegistry


def main() -> None:
    """Main function để chạy consumer."""
    # Config
    config = KafkaConfig(bootstrap_servers="localhost:9092")

    # Factories
    consumer_factory = KafkaConsumerFactory(config)
    producer_factory = KafkaProducerFactory(config)

    # DLQ Publisher (dùng chung cho tất cả handlers)
    dlq_producer = producer_factory.create()
    serializer = JsonSerializer()
    dlq_publisher = DeadLetterPublisher(dlq_producer, serializer)

    # Registry
    print("Starting consumer...")
    print("Press Ctrl+C to stop")
    registry = PredictionRegistry(consumer_factory, dlq_publisher)
    registry.register_all()
