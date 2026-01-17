"""
__main__ - Main entry point với options producer/consumer

Usage:
    python -m src producer    # Chạy producer
    python -m src consumer    # Chạy consumer
"""
import sys


def print_usage() -> None:
    """In hướng dẫn sử dụng."""
    print("Usage: python -m src <command>")
    print("")
    print("Commands:")
    print("  producer    Chạy Kafka producer (publish events)")
    print("  consumer    Chạy Kafka consumer (consume events)")


def main() -> None:
    """Main entry point."""
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "producer":
        from src.run_producer import main as run_producer
        run_producer()

    elif command == "consumer":
        from src.run_consumer import main as run_consumer
        run_consumer()

    else:
        print(f"Unknown command: {command}")
        print("")
        print_usage()
        sys.exit(1)


if __name__ == "__main__":
    main()
