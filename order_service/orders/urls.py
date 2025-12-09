from django.urls import path

from orders.views import OrderCreateView, OrderStatusView

urlpatterns = [
    path("orders/", OrderCreateView.as_view(), name="order-create"),
    path("orders/<int:pk>/", OrderStatusView.as_view(), name="order-status"),
]
