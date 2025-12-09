from django.db import models


class Payment(models.Model):
    order_id = models.IntegerField(unique=True)
    status = models.CharField(max_length=20)
    amount = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    reason = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        db_table = "payment_service_payments"


class PaymentInbox(models.Model):
    message_id = models.UUIDField(primary_key=True, editable=False)
    payload = models.JSONField()

    class Meta:
        db_table = "payment_service_inbox"
