import uuid
from unittest.mock import MagicMock, patch

from django.test import TestCase

from .management.commands.run_kafka_consumer import Command as KafkaConsumerCommand
from .models import Shipment, ShippingInbox


class ShippingConsumerTests(TestCase):
    """Тесты для Kafka-консьюмера сервиса доставки."""

    def setUp(self):
        self.consumer_command = KafkaConsumerCommand()

    def _create_mock_kafka_message(self, payload):
        message = MagicMock()
        message.topic = "payments"
        message.value = payload
        message.headers = [("message_id", str(uuid.uuid4()).encode("utf-8"))]
        return message

    @patch("shipments.management.commands.run_kafka_consumer.get_producer")
    def test_creates_shipment_on_paid_event(self, mock_get_producer):
        """Тест: создается отгрузка при получении события об успешной оплате."""
        mock_producer = MagicMock()
        mock_get_producer.return_value = mock_producer

        payload = {"orderId": 1, "status": "PAID", "amount": 150.0}
        message = self._create_mock_kafka_message(payload)

        with patch(
            "shipments.management.commands.run_kafka_consumer.get_consumer"
        ) as mock_get_consumer:
            mock_get_consumer.return_value = [message]
            self.consumer_command.handle()

        # Проверяем, что в БД создана отгрузка
        shipment = Shipment.objects.get(order_id=1)
        self.assertIsNotNone(shipment.tracking_number)
        self.assertTrue(shipment.tracking_number.startswith("SHIP-"))

        # Проверяем, что отправлено событие в топик 'shipments'
        mock_producer.send.assert_called_once()
        args, kwargs = mock_producer.send.call_args
        self.assertEqual(args[0], "shipments")
        sent_payload = kwargs["value"]
        self.assertEqual(sent_payload["orderId"], 1)
        self.assertEqual(sent_payload["status"], "SHIPPED")
        self.assertEqual(sent_payload["trackingNumber"], shipment.tracking_number)

        # Проверяем запись в inbox
        self.assertTrue(ShippingInbox.objects.exists())

    @patch("shipments.management.commands.run_kafka_consumer.get_producer")
    def test_ignores_non_paid_events(self, mock_get_producer):
        """Тест: консьюмер игнорирует события, не являющиеся успешной оплатой."""
        mock_producer = MagicMock()
        mock_get_producer.return_value = mock_producer

        payload = {"orderId": 2, "status": "SOME_OTHER_STATUS"}
        message = self._create_mock_kafka_message(payload)

        with patch(
            "shipments.management.commands.run_kafka_consumer.get_consumer"
        ) as mock_get_consumer:
            mock_get_consumer.return_value = [message]
            self.consumer_command.handle()

        # Убеждаемся, что ничего не было создано и отправлено
        self.assertEqual(Shipment.objects.count(), 0)
        mock_producer.send.assert_not_called()
        # Сообщение все равно должно попасть в inbox, чтобы не обрабатывать его повторно
        self.assertTrue(ShippingInbox.objects.exists())

    @patch("shipments.management.commands.run_kafka_consumer.get_producer")
    def test_consumer_is_idempotent(self, mock_get_producer):
        """Тест: консьюмер идемпотентен."""
        mock_producer = MagicMock()
        mock_get_producer.return_value = mock_producer

        payload = {"orderId": 3, "status": "PAID", "amount": 150.0}
        message = self._create_mock_kafka_message(payload)

        # Первый запуск
        with patch(
            "shipments.management.commands.run_kafka_consumer.get_consumer"
        ) as mock_get_consumer:
            mock_get_consumer.return_value = [message]
            self.consumer_command.handle()

        self.assertEqual(Shipment.objects.count(), 1)
        self.assertEqual(ShippingInbox.objects.count(), 1)
        mock_producer.send.assert_called_once()

        # Повторный запуск
        with patch(
            "shipments.management.commands.run_kafka_consumer.get_consumer"
        ) as mock_get_consumer:
            mock_get_consumer.return_value = [message]
            self.consumer_command.handle()

        self.assertEqual(Shipment.objects.count(), 1)
        self.assertEqual(ShippingInbox.objects.count(), 1)
        mock_producer.send.assert_called_once()
