import uuid
from unittest.mock import MagicMock, patch

from django.test import TestCase

from .management.commands.run_kafka_consumer import Command as KafkaConsumerCommand
from .models import Payment, PaymentInbox


class PaymentConsumerTests(TestCase):
    """Тесты для Kafka-консьюмера сервиса оплаты."""

    def setUp(self):
        self.consumer_command = KafkaConsumerCommand()

    def _create_mock_kafka_message(self, payload):
        message = MagicMock()
        message.topic = "orders"
        message.value = payload
        message.headers = [("message_id", str(uuid.uuid4()).encode("utf-8"))]
        return message

    @patch("payments.management.commands.run_kafka_consumer.get_producer")
    def test_process_successful_payment_for_odd_orderid(self, mock_get_producer):
        """Тест: успешная оплата для заказа с нечетным ID."""
        mock_producer = MagicMock()
        mock_get_producer.return_value = mock_producer

        payload = {"orderId": 1, "status": "CREATED"}
        message = self._create_mock_kafka_message(payload)

        with patch(
            "payments.management.commands.run_kafka_consumer.get_consumer"
        ) as mock_get_consumer:
            mock_get_consumer.return_value = [message]
            self.consumer_command.handle()

        # Проверяем, что в БД создан платеж со статусом PAID
        payment = Payment.objects.get(order_id=1)
        self.assertEqual(payment.status, "PAID")
        self.assertIsNotNone(payment.amount)

        # Проверяем, что отправлено событие в топик 'payments'
        mock_producer.send.assert_called_once_with(
            "payments",
            value={"orderId": 1, "status": "PAID", "amount": 150.0},
            headers=mock_producer.send.call_args[1]["headers"],
        )
        # Проверяем, что сообщение записано в inbox
        self.assertTrue(PaymentInbox.objects.exists())

    @patch("payments.management.commands.run_kafka_consumer.get_producer")
    def test_process_failed_payment_for_even_orderid(self, mock_get_producer):
        """Тест: ошибка оплаты для заказа с четным ID."""
        mock_producer = MagicMock()
        mock_get_producer.return_value = mock_producer

        payload = {"orderId": 2, "status": "CREATED"}
        message = self._create_mock_kafka_message(payload)

        with patch(
            "payments.management.commands.run_kafka_consumer.get_consumer"
        ) as mock_get_consumer:
            mock_get_consumer.return_value = [message]
            self.consumer_command.handle()

        # Проверяем, что в БД создан платеж со статусом CANCELLED
        payment = Payment.objects.get(order_id=2)
        self.assertEqual(payment.status, "CANCELLED")
        self.assertEqual(payment.reason, "INSUFFICIENT_FUNDS")

        # Проверяем, что отправлено событие в топик 'cancels'
        mock_producer.send.assert_called_once_with(
            "cancels",
            value={"orderId": 2, "status": "CANCELLED", "reason": "INSUFFICIENT_FUNDS"},
            headers=mock_producer.send.call_args[1]["headers"],
        )
        self.assertTrue(PaymentInbox.objects.exists())

    @patch("payments.management.commands.run_kafka_consumer.get_producer")
    def test_consumer_is_idempotent(self, mock_get_producer):
        """Тест: консьюмер идемпотентен и не обрабатывает дубликаты."""
        mock_producer = MagicMock()
        mock_get_producer.return_value = mock_producer

        payload = {"orderId": 3, "status": "CREATED"}
        message = self._create_mock_kafka_message(payload)

        # Первый запуск
        with patch(
            "payments.management.commands.run_kafka_consumer.get_consumer"
        ) as mock_get_consumer:
            mock_get_consumer.return_value = [message]
            self.consumer_command.handle()

        self.assertEqual(Payment.objects.count(), 1)
        self.assertEqual(PaymentInbox.objects.count(), 1)
        mock_producer.send.assert_called_once()

        # Повторный запуск
        with patch(
            "payments.management.commands.run_kafka_consumer.get_consumer"
        ) as mock_get_consumer:
            mock_get_consumer.return_value = [message]
            self.consumer_command.handle()

        # Количество объектов в БД и вызовов продюсера не должно измениться
        self.assertEqual(Payment.objects.count(), 1)
        self.assertEqual(PaymentInbox.objects.count(), 1)
        mock_producer.send.assert_called_once()
