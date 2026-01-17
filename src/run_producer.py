"""
run_producer - Entry point để chạy Kafka producer
"""
from src.infrastructure.KafkaConfig import KafkaConfig
from src.infrastructure.KafkaProducerFactory import KafkaProducerFactory
from src.shared.base.JsonSerializer import JsonSerializer
from src.domains.prediction.producer.PredictionPublisher import PredictionPublisher


def main() -> None:
    """Main function để chạy producer."""
    config = KafkaConfig(bootstrap_servers="localhost:9092")
    factory = KafkaProducerFactory(config)
    producer = factory.create()
    serializer = JsonSerializer()

    publisher = PredictionPublisher(producer, serializer)

    try:
        # Demo: publish một số events
        for i in range(3):
            publisher.create_event({
                "model_name": "sentiment",
                "input_text": f"Hello world {i}",
                "request_id": f"req-{i}",
            })
            print(f"Published event req-{i}")

        publisher.flush()
        print("All events flushed")

    finally:
        publisher.close()
        print("Producer closed")
