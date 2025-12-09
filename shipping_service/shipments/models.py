from django.db import models


class Shipment(models.Model):
    order_id = models.IntegerField(unique=True)
    tracking_number = models.CharField(max_length=100, unique=True)

    class Meta:
        db_table = "shipping_service_shipments"


class ShippingInbox(models.Model):
    message_id = models.UUIDField(primary_key=True, editable=False)
    payload = models.JSONField()

    class Meta:
        db_table = "shipping_service_inbox"
