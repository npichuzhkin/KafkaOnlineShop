package com.npichuzhkin.listeners;

import com.npichuzhkin.models.Order;
import com.npichuzhkin.repositories.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ShippingListener {
    private final KafkaTemplate<String, Order> kafkaTemplate;
    private final OrderRepository orderRepository;

    private static final Logger logger = LoggerFactory.getLogger(ShippingListener.class);

    public ShippingListener(KafkaTemplate<String, Order> kafkaTemplate, OrderRepository orderRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderRepository = orderRepository;
    }

    @KafkaListener(topics = "payed_orders", groupId = "shipping-group")
    public void listenForPayedOrders(Order order) {
        Order existingOrder = orderRepository.findById(order.getId()).orElse(null);

        if (existingOrder != null) {
            existingOrder.setStatus("SHIPPED");
            orderRepository.save(existingOrder);
            kafkaTemplate.send("sent_orders", existingOrder);
            logger.info("Заказ " + order.getId() + " отгружен и отправлен в топик sent_orders");
        } else {
            System.out.println("Order not found: " + order.getId());
        }
    }
}

