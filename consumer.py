#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 22 11:28:49 2021

@author: mockingbird
"""
from pprint import pprint
from kafka import KafkaConsumer

class Consummer:
    def __init__(self):
        self.server = 'localhost:9092'
        # self.server = "172.30.0.3:9093"
        self.topic = "test"
        self.consumer = KafkaConsumer(self.topic, auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group-1', bootstrap_servers=self.server)
        self.consumer.subscribe([self.topic])
        self.run = True

    def runConsummer(self):
        while self.run:
            for msg in self.consumer:
                pprint("{}, Size => {}".format(msg.value, len(msg.value)))
            
        self.consumer.close()
        
if __name__ == "__main__":
    Consummer().runConsummer()