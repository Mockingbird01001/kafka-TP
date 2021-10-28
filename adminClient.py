#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 28 10:53:38 2021

@author: mockingbird
"""

from kafka import KafkaAdminClient
from kafka.admin import NewTopic

class Main:
    def __init__(self):
        # self.server = '10.92.15.53:9092'
        self.server = 'kafka:9093'
        self.admin = KafkaAdminClient(bootstrap_servers = self.server)
        
        self.topic = NewTopic(name='Hello-Kafka', num_partitions=3, replication_factor=1)
        self.admin.create_topics([self.topic])
    
if __name__ == "__main__":
    Main()