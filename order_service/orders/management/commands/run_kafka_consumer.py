import logging
import uuid

from django.core.management.base import BaseCommand
from django.db import IntegrityError, transaction
from orders.kafka_client.client import get_consumer
from orders.models import Order, OrderInbox

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Runs Kafka consumer for order updates"

    def handle(self, *args, **options):
        consumer = get_consumer(
            "payments", "shipments", "cancels", group_id="order_service_group"
        )
        self.stdout.write("Starting Kafka consumer for Order Service...")
        for message in consumer:
            header = next((h for h in message.headers if h[0] == "message_id"), None)
            if not header:
                continue

            try:
                message_id = uuid.UUID(header[1].decode("utf-8"))
                with transaction.atomic():
                    if OrderInbox.objects.filter(message_id=message_id).exists():
                        continue
                    OrderInbox.objects.create(
                        message_id=message_id, payload=message.value
                    )

                    order_id = message.value.get("orderId")
                    order = Order.objects.get(pk=order_id)

                    if message.topic == "payments":
                        order.status = Order.Status.PAID
                    elif message.topic == "shipments":
                        order.status = Order.Status.SHIPPED
                    elif message.topic == "cancels":
                        order.status = Order.Status.CANCELLED

                    order.save()
                    logger.info(f"Order {order_id} status updated to {order.status}")

            except (IntegrityError, Order.DoesNotExist, ValueError) as e:
                logger.error(f"Error processing message {message_id}: {e}")
