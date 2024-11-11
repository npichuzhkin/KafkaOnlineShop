package com.npichuzhkin.listeners;

import com.npichuzhkin.models.Order;
import com.npichuzhkin.repositories.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentListener {
    private final KafkaTemplate<String, Order> kafkaTemplate;
    private final OrderRepository orderRepository;

    private static final Logger logger = LoggerFactory.getLogger(PaymentListener.class);

    public PaymentListener(KafkaTemplate<String, Order> kafkaTemplate, OrderRepository orderRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderRepository = orderRepository;
    }

    @KafkaListener(topics = "new_orders", groupId = "payment-group")
    public void listenForNewOrders(Order order) {
        Order existingOrder = orderRepository.findById(order.getId()).orElse(null);

        if (existingOrder != null) {
            existingOrder.setStatus("PAID");
            orderRepository.save(existingOrder);
            kafkaTemplate.send("payed_orders", existingOrder);
            logger.info("Заказ " + order.getId() + " оплачен и отправлен в топик payed_orders");
        } else {
            System.out.println("Order not found: " + order.getId());
        }
    }
}

