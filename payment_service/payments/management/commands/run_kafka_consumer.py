import logging
import uuid

from django.core.management.base import BaseCommand
from django.db import IntegrityError, transaction
from payments.kafka_client.client import (
    generate_message_id_header,
    get_consumer,
    get_producer,
)
from payments.models import Payment, PaymentInbox

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Runs Kafka consumer for payment processing"

    def handle(self, *args, **options):
        consumer = get_consumer("orders", group_id="payment_service_group")
        producer = get_producer()
        self.stdout.write("Starting Kafka consumer for Payment Service...")
        for message in consumer:
            header = next((h for h in message.headers if h[0] == "message_id"), None)
            if not header:
                continue

            try:
                message_id = uuid.UUID(header[1].decode("utf-8"))
                with transaction.atomic():
                    if PaymentInbox.objects.filter(message_id=message_id).exists():
                        continue
                    PaymentInbox.objects.create(
                        message_id=message_id, payload=message.value
                    )

                    order_id = message.value.get("orderId")
                    if order_id % 2 == 1:
                        Payment.objects.create(
                            order_id=order_id, status="PAID", amount=150.0
                        )
                        payload = {
                            "orderId": order_id,
                            "status": "PAID",
                            "amount": 150.0,
                        }
                        producer.send(
                            "payments",
                            value=payload,
                            headers=generate_message_id_header(),
                        )
                        logger.info(f"Order {order_id} PAID.")
                    else:
                        Payment.objects.create(
                            order_id=order_id,
                            status="CANCELLED",
                            reason="INSUFFICIENT_FUNDS",
                        )
                        payload = {
                            "orderId": order_id,
                            "status": "CANCELLED",
                            "reason": "INSUFFICIENT_FUNDS",
                        }
                        producer.send(
                            "cancels",
                            value=payload,
                            headers=generate_message_id_header(),
                        )
                        logger.info(f"Order {order_id} CANCELLED.")

                    producer.flush()  # Отправляем и ждём подтверждения

            except (IntegrityError, ValueError) as e:
                logger.error(f"Error processing message: {e}")
