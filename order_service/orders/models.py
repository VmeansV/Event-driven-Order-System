import uuid

from django.db import models


class Order(models.Model):
    class Status(models.TextChoices):
        CREATED = "CREATED", "Created"
        PAID = "PAID", "Paid"
        SHIPPED = "SHIPPED", "Shipped"
        CANCELLED = "CANCELLED", "Cancelled"

    status = models.CharField(
        max_length=20, choices=Status.choices, default=Status.CREATED
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "order_service_orders"

    def __str__(self):
        return f"Order {self.id} - Status: {self.status}"


class OrderInbox(models.Model):
    message_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    payload = models.JSONField()
    processed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "order_service_inbox"


class OrderOutbox(models.Model):
    id = models.BigAutoField(primary_key=True)
    event_id = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    payload = models.JSONField()
    topic = models.CharField(max_length=100)
    headers = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)
    processed_at = models.DateTimeField(null=True, blank=True, db_index=True)

    class Meta:
        db_table = "order_service_outbox"

    def __str__(self):
        status = "Processed" if self.processed_at else "Pending"
        return f"Event {self.event_id} for topic '{self.topic}' ({status})"
