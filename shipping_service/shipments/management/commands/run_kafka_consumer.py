import logging
import uuid

from django.core.management.base import BaseCommand
from django.db import IntegrityError, transaction
from shipments.kafka_client.client import (
    generate_message_id_header,
    get_consumer,
    get_producer,
)
from shipments.models import Shipment, ShippingInbox

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Runs Kafka consumer for shipment processing"

    def handle(self, *args, **options):
        consumer = get_consumer("payments", group_id="shipping_service_group")
        producer = get_producer()
        self.stdout.write("Starting Kafka consumer for Shipping Service...")
        for message in consumer:
            header = next((h for h in message.headers if h[0] == "message_id"), None)
            if not header or message.value.get("status") != "PAID":
                continue

            try:
                message_id = uuid.UUID(header[1].decode("utf-8"))
                with transaction.atomic():
                    if ShippingInbox.objects.filter(message_id=message_id).exists():
                        continue
                    ShippingInbox.objects.create(
                        message_id=message_id, payload=message.value
                    )

                    order_id = message.value.get("orderId")
                    tracking_number = f"SHIP-{uuid.uuid4().hex[:10].upper()}"
                    Shipment.objects.create(
                        order_id=order_id, tracking_number=tracking_number
                    )

                    payload = {
                        "orderId": order_id,
                        "status": "SHIPPED",
                        "trackingNumber": tracking_number,
                    }
                    producer.send(
                        "shipments", value=payload, headers=generate_message_id_header()
                    )
                    producer.flush()
                    logger.info(
                        f"Order {order_id} SHIPPED with tracking {tracking_number}."
                    )
            except (IntegrityError, ValueError) as e:
                logger.error(f"Error processing message: {e}")
