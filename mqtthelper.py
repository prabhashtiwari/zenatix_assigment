import paho.mqtt.client as mqttClient
import json
import os
import sys
from threading import Thread
import subprocess
import time
import settings
import csv 
import json 


class MQTT:

    def __init__(self,clientId,host,port,username,password,TopicIn,TopicOut,SensorData,BufferData):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.clientId = clientId
        self.TopicIn = TopicIn
        self.TopicOut = TopicOut
        self.isConnected = False
        self.flag = False
        self.connect_flag=False
        self.SensorData=SensorData
        self.BufferData=BufferData
        self.Response=""
        global thread_flag
        try:
            self.connectBroker()
        except Exception as e:
            print ("MQTT not connected as "+str(e))

        # "Read csv file only one time and write data into array" 
        with open('dataset.csv') as f:
            self.SensorData = [{k: str(v) for k, v in row.items()}
                for row in csv.DictReader(f, skipinitialspace=True)]

        # "Thread started and sent sensor data to cloud every minute" 
        data_thread = Thread(target=self.data_thread)
        data_thread.daemon=True
        data_thread.start()
        
    def data_thread(self):
        for data in self.SensorData:
            print(data)
            self.publish_sync(self.TopicOut,str(data))
            print("Data Published Successfully.")
            time.sleep(60)
        
    def connectBroker(self):
        self.mqttClient = mqttClient.Client(self.clientId+"_mqtt")
        self.mqttClient.username_pw_set(self.username, password=self.password)
        self.mqttClient.on_connect = self.on_connect
        self.mqttClient.on_message = self.on_message     
        self.mqttClient.connect(self.host, port=int(self.port))
        self.mqttClient.loop_start()
        
        
    def on_connect(self, client, userdata, flags, rc):
        print("MQTT Application Connected!")
        self.mqttClient.subscribe(self.TopicIn)
        self.mqttClient.subscribe(self.pingTopic)
        print("Subscription Topic : "+str(self.TopicIn))
        print("Publish Topic : "+str(self.TopicOut))
        self.connect_flag = True

    def on_message(self, message):
        # "Received message from cloud" 
        print("Message received : " + message.payload)
        try:
            Obj = json.loads(message.payload)
            self.Response=obj["data"]
            if(obj["statusCode"]=="200"):

                # "Checking any buffered data is present or not if yes sent to cloud" 
                if(len(self.BufferData)==0):
                    pass
                else:
                    # "Publishing data to cloud" 
                    self.publish_sync(self.TopicOut,str(BufferData))
            else:
                pass
        except Exception as e:
            print(e)
        

    def publish_sync(self,pubTopic,data):
        self.Response=""
        # "Publish data to cloud" 
        self.mqttClient.publish(pubTopic,data)
        # "It check message received from cloud or not , if not append data into buffered data" 
        counter=0
        while(counter<50 and self.Response==""):
            time.sleep(0.1)
            counter+=1
        if(counter==50):
            self.BufferData.append(data)
        else:
            pass


		
