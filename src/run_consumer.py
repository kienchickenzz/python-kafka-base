"""
run_consumer - Entry point để chạy Kafka consumer
"""
from src.infrastructure.KafkaConfig import KafkaConfig
from src.infrastructure.KafkaConsumerFactory import KafkaConsumerFactory
from src.domains.prediction.consumer.PredictionRegistry import PredictionRegistry


def main() -> None:
    """Main function để chạy consumer."""
    config = KafkaConfig(bootstrap_servers="localhost:9092")
    factory = KafkaConsumerFactory(config)

    print("Starting consumer...")
    registry = PredictionRegistry(factory)
    registry.register_all()
