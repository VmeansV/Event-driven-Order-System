import uuid
from unittest.mock import MagicMock, patch

from django.test import TestCase
from django.urls import reverse
from rest_framework import status

from orders.management.commands.run_kafka_consumer import (
    Command as KafkaConsumerCommand,
)
from orders.models import Order, OrderInbox


class OrderAPITests(TestCase):
    """Тесты для REST API сервиса заказов."""

    @patch("orders.views.get_producer")
    def test_create_order_success(self, mock_get_producer):
        """
        Тест: успешное создание заказа через POST /orders/.
        Проверяем:
        1. Возвращается статус 201 CREATED.
        2. Заказ создается в БД со статусом CREATED.
        3. Вызывается Kafka-продюсер для отправки события с правильным payload.
        """
        # Настраиваем мок-продюсера
        mock_producer = MagicMock()
        mock_get_producer.return_value = mock_producer

        # Выполняем POST-запрос
        url = reverse("order-create")
        response = self.client.post(url, {}, format="json")

        # 1. Проверяем HTTP-ответ
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        # 2. Проверяем, что заказ создан в БД
        self.assertTrue(Order.objects.filter(pk=response.data["id"]).exists())
        order = Order.objects.get(pk=response.data["id"])
        self.assertEqual(order.status, Order.Status.CREATED)

        # 3. Проверяем, что было отправлено событие в Kafka
        mock_producer.send.assert_called_once()
        # Получаем аргументы, с которыми был вызван mock_producer.send()
        call_args, call_kwargs = mock_producer.send.call_args

        # Проверяем топик
        self.assertEqual(call_args[0], "orders")

        # Проверяем payload (содержимое сообщения)
        sent_payload = call_kwargs["value"]
        self.assertIn("orderId", sent_payload)
        self.assertIn("status", sent_payload)
        self.assertEqual(sent_payload["orderId"], order.id)
        self.assertEqual(sent_payload["status"], "CREATED")

    def test_get_order_status_success(self):
        """
        Тест: успешное получение статуса существующего заказа через GET /orders/{id}/.
        """
        order = Order.objects.create(status=Order.Status.PAID)
        url = reverse("order-status", kwargs={"pk": order.id})
        response = self.client.get(url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], order.id)
        self.assertEqual(response.data["status"], Order.Status.PAID)

    def test_get_order_status_not_found(self):
        """
        Тест: получение статуса несуществующего заказа возвращает 404.
        """
        url = reverse("order-status", kwargs={"pk": 999})
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


class OrderConsumerTests(TestCase):
    """Тесты для Kafka-консьюмера сервиса заказов."""

    def setUp(self):
        """Настройка, выполняемая перед каждым тестом в этом классе."""
        self.consumer_command = KafkaConsumerCommand()
        self.order = Order.objects.create()

    def _create_mock_kafka_message(self, topic, payload):
        """Вспомогательная функция для создания мок-сообщения Kafka."""
        message = MagicMock()
        message.topic = topic
        message.value = payload
        # Генерируем уникальный message_id для каждого сообщения
        message.headers = [("message_id", str(uuid.uuid4()).encode("utf-8"))]
        return message

    @patch("orders.management.commands.run_kafka_consumer.get_consumer")
    def test_consumer_updates_status_to_paid(self, mock_get_consumer):
        """Тест: консьюмер обновляет статус заказа на PAID при событии из 'payments'."""
        payload = {"orderId": self.order.id, "status": "PAID"}
        message = self._create_mock_kafka_message("payments", payload)
        mock_get_consumer.return_value = [
            message
        ]  # Имитируем получение одного сообщения

        # Запускаем обработчик
        self.consumer_command.handle()

        self.order.refresh_from_db()
        self.assertEqual(self.order.status, Order.Status.PAID)
        # Проверяем, что сообщение было записано в inbox
        self.assertEqual(OrderInbox.objects.count(), 1)

    @patch("orders.management.commands.run_kafka_consumer.get_consumer")
    def test_consumer_updates_status_to_shipped(self, mock_get_consumer):
        """Тест: консьюмер обновляет статус заказа на SHIPPED
        при событии из 'shipments'."""
        # Устанавливаем начальное состояние
        self.order.status = Order.Status.PAID
        self.order.save()

        payload = {
            "orderId": self.order.id,
            "status": "SHIPPED",
            "trackingNumber": "TRACK123",
        }
        message = self._create_mock_kafka_message("shipments", payload)
        mock_get_consumer.return_value = [message]

        # Запускаем обработчик
        self.consumer_command.handle()

        self.order.refresh_from_db()
        self.assertEqual(self.order.status, Order.Status.SHIPPED)
        self.assertEqual(OrderInbox.objects.count(), 1)

    @patch("orders.management.commands.run_kafka_consumer.get_consumer")
    def test_consumer_is_idempotent(self, mock_get_consumer):
        """Тест: консьюмер обрабатывает одно и то же
        сообщение только один раз (идемпотентность)."""
        payload = {"orderId": self.order.id, "status": "PAID"}
        message = self._create_mock_kafka_message("payments", payload)

        # Имитируем получение одного и того же сообщения дважды
        mock_get_consumer.return_value = [message, message]

        # Запускаем обработчик
        self.consumer_command.handle()

        # Проверяем, что статус обновился
        self.order.refresh_from_db()
        self.assertEqual(self.order.status, Order.Status.PAID)

        # Проверяем, что в inbox только ОДНА запись,
        # несмотря на два одинаковых сообщения
        self.assertEqual(OrderInbox.objects.count(), 1)
