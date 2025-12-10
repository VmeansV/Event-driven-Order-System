import logging
import time

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from kafka.errors import KafkaError
from orders.kafka_client.client import get_producer
from orders.models import OrderOutbox

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Runs the Outbox relay to send pending messages to Kafka"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting Outbox relay..."))

        producer = get_producer()
        if not producer:
            self.stderr.write(
                self.style.ERROR("Could not get Kafka producer. Relay will not start.")
            )
            return

        while True:
            try:
                messages_to_send = []
                with transaction.atomic():
                    messages_to_send = list(
                        OrderOutbox.objects.select_for_update(skip_locked=True)
                        .filter(processed_at__isnull=True)
                        .order_by("created_at")[:100]
                    )

                    if not messages_to_send:
                        time.sleep(1)
                        continue

                    self.stdout.write(
                        f"Found {len(messages_to_send)} messages to send."
                    )

                    futures = []
                    for msg in messages_to_send:
                        headers = msg.headers if msg.headers else []
                        if not any(h[0] == "message_id" for h in headers):
                            headers.append(
                                ("message_id", str(msg.event_id).encode("utf-8"))
                            )

                        future = producer.send(
                            msg.topic, value=msg.payload, headers=headers
                        )
                        futures.append(future)

                    producer.flush()

                    for future in futures:
                        future.get(timeout=10)

                    message_ids = [msg.id for msg in messages_to_send]
                    OrderOutbox.objects.filter(id__in=message_ids).update(
                        processed_at=timezone.now()
                    )

                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Successfully sent and marked {len(messages_to_send)} messages."
                        )
                    )

            except KafkaError as e:
                logger.error(
                    f"Failed to send batch to Kafka: {e}. Retrying in 5 seconds..."
                )
                time.sleep(5)

            except Exception as e:
                logger.error(
                    f"An unexpected error occurred in the relay main loop: {e}"
                )
                time.sleep(5)
