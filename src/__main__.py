"""
Main entry point
Usage:
    python -m src --producer    # Chạy producer
    python -m src --consumer    # Chạy consumer
"""
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Demo")
    group = parser.add_mutually_exclusive_group(required=True)

    # Run Mode
    group.add_argument("--producer", action="store_true", help="Run Kafka producer")
    group.add_argument("--consumer", action="store_true", help="Run Kafka consumer")

    options = parser.parse_args()

    if options.producer:
        from src.run_producer import main as run_producer
        run_producer()

    elif options.consumer:
        from src.run_consumer import main as run_consumer
        run_consumer()
