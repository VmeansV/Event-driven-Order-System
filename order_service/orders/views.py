import logging

from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from orders.kafka_client.client import generate_message_id_header, get_producer
from orders.models import Order
from orders.serializers import OrderSerializer

logger = logging.getLogger(__name__)


class OrderCreateView(APIView):
    def post(self, request, *args, **kwargs):
        order = Order.objects.create(status=Order.Status.CREATED)
        produser = get_producer()
        if produser:
            message = {"orderId": order.id, "status": order.status}
            headers = generate_message_id_header()
            produser.send("orders", value=message, headers=headers)
            produser.flush()
            logger.info(f"Sent OrderCreated event for order {order.id}")
        serializer = OrderSerializer(order)
        return Response(serializer.data, status=status.HTTP_201_CREATED)


class OrderStatusView(APIView):
    def get(self, request, pk, *args, **kwargs):
        order = get_object_or_404(Order, pk=pk)
        serializer = OrderSerializer(order)
        return Response(serializer.data)
