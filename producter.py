#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 22 10:49:25 2021

@author: mockingbird
"""

from kafka import KafkaProducer    
    
class Producer:
    def __init__(self):
        self.server = 'kafka:9093'
        self.producer = KafkaProducer(bootstrap_servers = self.server)
        self.topic = "Hello-Kafka"
        self.run = True
        
    def runProducer(self):
        while self.run:
             self.producer.send(self.topic, b'First  message !')
             self.producer.send(self.topic, b'Second message !')
             self.producer.send(self.topic, b'Third  message !')
             self.producer.send(self.topic, b'Fourth message !')
             
        self.producer.close()
        
if __name__ == "__main__":
    Producer().runProducer()
