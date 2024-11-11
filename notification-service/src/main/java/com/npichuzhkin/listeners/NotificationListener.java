package com.npichuzhkin.listeners;

import com.npichuzhkin.models.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationListener {

    private static final Logger logger = LoggerFactory.getLogger(NotificationListener.class);

    @KafkaListener(topics = "sent_orders", groupId = "notification-group")
    public void listenForShippedOrders(Order order) {
        logger.info("Уведомление об отправке заказа " + order.getId() + " отправлено пользователю");
        System.out.println("Уведомление пользователю: Заказ " + order.getId() + " успешно отправлен.");
    }
}

