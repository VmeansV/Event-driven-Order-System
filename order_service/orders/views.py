import logging
import uuid

from django.db import transaction
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from orders.models import Order
from orders.serializers import OrderSerializer

logger = logging.getLogger(__name__)


class OrderCreateView(APIView):
    def post(self, request, *args, **kwargs):
        event_id = uuid.uuid4()
        topic = "orders"
        with transaction.atomic():
            order = Order.objects.create(status=Order.Status.CREATED)

            message_payload = {"orderId": order.id, "status": order.status}

            message_headers = {"message_id": str(event_id)}

            Order.objects.create(
                event_id=event_id,
                topic=topic,
                headers=message_headers,
                payload=message_payload,
            )

        serializer = OrderSerializer(order)
        return Response(serializer.data, status=status.HTTP_201_CREATED)


class OrderStatusView(APIView):
    def get(self, request, pk, *args, **kwargs):
        order = get_object_or_404(Order, pk=pk)
        serializer = OrderSerializer(order)
        return Response(serializer.data)
