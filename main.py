#!/usr/bin/python
import time
import os
import mqtthelper
from threading import Thread
import settings
import subprocess

settings.init()
print("Init...")
print("Check CPU: MQTT support: yes")

def mqtt_thread():
    obj=mqtthelper.MQTT(settings.CLIENT_ID,settings.SERVER_IP,settings.SERVER_PORT,settings.MQTT_UNAM,settings.MQTT_UPSK,settings.TOPIC_IN,settings.TOPIC_OUT,settings.SENSOR_DATA,settings.BUFFERED_DATA)

# "Mqtt thread started 
mqtt_thread = Thread(target=mqtt_thread)
mqtt_thread.start()

while True:
    try:
        time.sleep(1)
    except:
        mqtt_thread.join()
        print("MQTT Application Stopped!")
        raise SystemExit
    
