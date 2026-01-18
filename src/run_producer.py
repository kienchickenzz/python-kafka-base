"""
run_producer - Entry point để chạy Kafka producer

Trách nhiệm:
- Publish test events bao gồm cả success và fail cases
"""
from src.producer.infrastructure.KafkaConfig import KafkaConfig
from src.producer.infrastructure.KafkaProducerFactory import KafkaProducerFactory
from src.shared.base.JsonSerializer import JsonSerializer
from src.producer.prediction.PredictionPublisher import PredictionPublisher


# Test messages: bao gồm success và fail cases
TEST_MESSAGES = [
    # Success cases
    {
        "request_id": "req-001",
        "model_name": "sentiment",
        "input_text": "Hello world, this is a test message",
    },
    {
        "request_id": "req-002",
        "model_name": "classification",
        "input_text": "Another valid message for testing",
    },

    # Fail case 1: model_name = "fail" → ValueError
    {
        "request_id": "req-003-fail",
        "model_name": "fail",
        "input_text": "This will fail because model is 'fail'",
    },

    # Success case
    {
        "request_id": "req-004",
        "model_name": "sentiment",
        "input_text": "This should succeed",
    },

    # Fail case 2: input_text chứa "error" → RuntimeError
    {
        "request_id": "req-005-fail",
        "model_name": "sentiment",
        "input_text": "This message contains error word",
    },

    # Success case
    {
        "request_id": "req-006",
        "model_name": "ner",
        "input_text": "Final success message",
    },

    # Fail case 3: input_text rỗng → ValueError
    {
        "request_id": "req-007-fail",
        "model_name": "sentiment",
        "input_text": "",
    },
]


def main() -> None:
    """Main function để chạy producer."""
    config = KafkaConfig(bootstrap_servers="localhost:9092")
    factory = KafkaProducerFactory(config)
    producer = factory.create()
    serializer = JsonSerializer()

    publisher = PredictionPublisher(producer, serializer)

    try:
        print("Publishing test messages...")
        print("=" * 50)

        for msg in TEST_MESSAGES:
            request_id = msg["request_id"]
            model_name = msg["model_name"]

            # Đánh dấu message sẽ fail
            will_fail = (
                model_name == "fail" or
                "error" in msg["input_text"].lower() or
                not msg["input_text"]
            )
            status = "WILL FAIL" if will_fail else "OK"

            publisher.create_event(msg)
            print(f"Published: {request_id} [{status}]")

        publisher.flush()
        print("=" * 50)
        print(f"Total: {len(TEST_MESSAGES)} messages published")
        print("Success expected: 4, Fail expected: 3")

    finally:
        publisher.close()
        print("Producer closed")
