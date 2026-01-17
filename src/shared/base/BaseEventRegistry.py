"""
BaseEventRegistry - Registry pattern cho Kafka consumer handlers

Trách nhiệm:
- Quản lý danh sách handlers
- Dùng factory để tạo consumer cho từng handler
- Mỗi handler chạy polling ở thread riêng
- Graceful shutdown tất cả threads
"""
import signal
import threading
from abc import ABC, abstractmethod
from kafka import KafkaConsumer

from src.infrastructure.KafkaConsumerFactory import KafkaConsumerFactory
from src.shared.interface.IEventHandler import IEventHandler


class BaseEventRegistry(ABC):
    """
    Base Registry cho Kafka event handlers.

    Subclass phải implement _create_handlers() để định nghĩa handlers.

    Example:
        class PredictionRegistry(BaseEventRegistry):
            def _create_handlers(self) -> list[IEventHandler]:
                return [
                    PredictionHandler(),
                    AnalyticsHandler(),
                ]

        # Usage
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        factory = KafkaConsumerFactory(config)
        registry = PredictionRegistry(factory)
        registry.register_all()
    """

    def __init__(self, consumer_factory: KafkaConsumerFactory) -> None:
        """
        Khởi tạo registry với consumer factory.

        Args:
            consumer_factory (KafkaConsumerFactory): Factory để tạo consumers
        """
        self._factory = consumer_factory
        self._handlers: dict[str, IEventHandler] = {}
        self._consumers: list[KafkaConsumer] = []
        self._threads: list[threading.Thread] = []
        self._running = False

        # Get handlers from subclass
        handlers = self._create_handlers()
        for handler in handlers:
            self._handlers[handler.topic.value] = handler

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    @abstractmethod
    def _create_handlers(self) -> list[IEventHandler]:
        """
        Tạo và trả về danh sách handlers.

        Subclass PHẢI implement method này.

        Returns:
            list[IEventHandler]: Danh sách handler instances
        """
        pass

    def _signal_handler(self, signum, frame) -> None:
        """Handle shutdown signals."""
        self._running = False

    def _run_consumer(self, handler: IEventHandler, consumer: KafkaConsumer) -> None:
        """
        Polling loop cho một handler (chạy trong thread riêng).

        Args:
            handler (IEventHandler): Handler để xử lý messages
            consumer (KafkaConsumer): Consumer để poll
        """
        try:
            while self._running:
                records = consumer.poll(timeout_ms=1000) # 1 second
                if not records:
                    continue

                for topic_partition, messages in records.items():
                    for msg in messages:
                        try:
                            handler.handle(msg.value)
                            consumer.commit()
                        except Exception:
                            # Log lỗi nhưng không dừng consumer
                            print(f"Error processing message: {msg.value}")
                            # TODO: Đẩy vào DLQ nếu cần thiết
                            consumer.commit()
        finally:
            consumer.close()

    def _register_handler(self, handler: IEventHandler) -> None:
        """
        Tạo consumer từ factory và start thread cho handler.

        Args:
            handler (IEventHandler): Handler cần register
        """
        consumer = self._factory.create(handler)
        self._consumers.append(consumer)

        thread = threading.Thread(
            target=self._run_consumer,
            args=(handler, consumer),
            name=f"consumer-{handler.topic.value}",
            daemon=True,
        )
        self._threads.append(thread)
        thread.start()

    def register_all(self) -> None:
        """
        Start tất cả handlers trong các threads riêng biệt.

        Method này sẽ block cho đến khi nhận signal shutdown.
        """
        self._running = True

        for handler in self._handlers.values():
            self._register_handler(handler)

        # Block main thread, chờ shutdown signal
        try:
            while self._running:
                threading.Event().wait(1)
        finally:
            self._shutdown()

    def _shutdown(self) -> None:
        """Graceful shutdown tất cả threads."""
        self._running = False
        for thread in self._threads:
            thread.join(timeout=5)
