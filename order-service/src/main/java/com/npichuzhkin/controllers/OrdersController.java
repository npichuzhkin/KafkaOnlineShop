package com.npichuzhkin.controllers;

import com.npichuzhkin.dto.OrderDTO;
import com.npichuzhkin.models.Order;
import com.npichuzhkin.repositories.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrdersController {
    private final KafkaTemplate<String, Order> kafkaTemplate;

    private final OrderRepository orderRepository;

    private static final Logger logger = LoggerFactory.getLogger(OrdersController.class);

    @Autowired
    public OrdersController(KafkaTemplate<String, Order> kafkaTemplate, OrderRepository orderRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderRepository = orderRepository;
    }

    @PostMapping
    public ResponseEntity<Void> createOrder(@RequestBody OrderDTO orderDTO) {

        Order newOrder = new Order();
        newOrder.setName(orderDTO.getName());
        newOrder.setStatus("NEW");
        newOrder = orderRepository.save(newOrder);

        kafkaTemplate.send("new_orders", newOrder);
        logger.info("Новый заказ " + newOrder.getId() + " отправлен в топик new_orders");
        return ResponseEntity.ok().build();
    }
}

