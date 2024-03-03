#!/usr/bin/python3

#
# MQTT Weather script, get Fineoffset weather sensor data from OpenMQTT
# Author: Matt Way, matt@econode.nz
# Date 2024-03-03

# Standard / included modules
import os, datetime, json, time, subprocess

# Modules need to be installed with pip package manager
import paho.mqtt.client as mqtt
import yaml
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


## Functions begin
def setupMqtt():
    global client
    mqtt_host = confData['mqtt']['host']
    mqtt_port = confData['mqtt']['port']
    client = mqtt.Client()
    client.hostConnected = False
    client.on_connect=mqtt_on_connect
    client.username_pw_set(confData['mqtt']['user'],confData['mqtt']['password'])
    
    try:
        client.connect(mqtt_host, mqtt_port)
    except:
        print( f"Error: MQTT, host or port failure using host={mqtt_host} port={mqtt_port}" )
        return

def mqtt_on_connect(client, userdata, flags, rc):
    print(f"MQTT Connect code: {rc}")
    if rc==0:
        client.hostConnected = True
        mqtt_subscribe()
    if rc==1 or rc==2 or rc==3:
        print("ERROR: MQTT server unavailable")
    if rc==4 or rc==5:
        print("ERROR: MQTT not authorised")

def mqtt_on_message(client, userdata, message):
    mqtt_payload = str(message.payload.decode("utf-8"))
    rawData = json.loads( mqtt_payload )
    masage_data( rawData )

def mqtt_subscribe():
    open_mqtt_topic = confData['mqtt']['open_mqtt_topic']
    print( f"MQTT subscribing to topic: {open_mqtt_topic}" )
    client.on_message = mqtt_on_message
    client.subscribe(open_mqtt_topic)

def getCardinalPoint16( deg ):
    cPoints = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW']
    if deg < 0 or deg > 347:
        deg = 0
    idx = round(deg/22.5)
    return cPoints[idx]

def masage_data( rawData ):
    # Weather data packet looks like this;
    # {'model': 'Fineoffset-WHx080', 'subtype': 0, 'id': 153, 'battery_ok': 1, 'temperature_C': 21.3,
    # 'humidity': 83, 'wind_dir_deg': 158, 'wind_avg_km_h': 1.224, 'wind_max_km_h': 3.672,
    # 'rain_mm': 14.4, 'mic': 'CRC', 'protocol': 'Fine Offset Electronics WH1080/WH3080 Weather Station',
    # 'rssi': -65, 'duration': 390000}

    # Check we have the correct model.
    try:
        model = rawData['model']
        if model != 'Fineoffset-WHx080':
            return
        now = datetime.datetime.now()
        nowTimeStamp = now.strftime("%Y-%m-%d %H:%M:%S")
        sensorData['timeStamp'] = nowTimeStamp
        sensorData['batt_ok'] = int(rawData['battery_ok'])
        sensorData['humidity'] = int(rawData['humidity'])
        sensorData['temp_c'] = float(rawData['temperature_C'])
        sensorData['wind_speed'] = float(rawData['wind_avg_km_h'])
        sensorData['max_wind_speed'] = float(rawData['wind_max_km_h'])
        sensorData['wind_direction'] = int(rawData['wind_dir_deg'])
        sensorData['rain_mm'] = float(rawData['rain_mm'])
        sensorData['rssi'] = int(rawData['rssi'])
        sensorData['cp16'] = getCardinalPoint16(rawData['wind_dir_deg'])
    except:
        return
    printSensorData()
    if is_csv_enabled:
        writeCsv()
        deleteOldCsvFiles()
    if is_influxdb_enabled:
        pushInfluxData()


def writeCsv():
    now = datetime.datetime.now()
    nowDate = now.strftime("%Y-%m-%d")
    csv_path = f"{confData['csv']['path']}/"
    csv_file_name = f"weather_{project_name}_{nowDate}.csv"
    if os.path.exists(csv_path+csv_file_name):
        fp_csv = open( csv_path+csv_file_name,"a" )
    else:
        fp_csv = open( csv_path+csv_file_name,"w+" )
        fp_csv.write( getCsvHeader() )
    fp_csv.write( getCsvLine() )
    fp_csv.close()

def getCsvHeader():
    return '"time_stamp", "batt_ok", "humidity", "temp_c", "wind_speed", "max_wind_speed","cp16","wind_direction", "rain_mm", "rssi"\r\n'

def getCsvLine():
    line = '{timeStamp}, {batt_ok}, {humidity}, {temp_c}, {wind_speed}, {max_wind_speed},"{cp16}",{wind_direction}, {rain_mm}, {rssi}\r\n'
    return line.format_map(sensorData)

def deleteOldCsvFiles():
    csv_path = f"{confData['csv']['path']}/"
    csv_retention_days = int(confData['csv']['retention_days'])
    for fileName in os.listdir(csv_path):
        fileAgeInDays =  round( ( time.time() - os.path.getmtime( csv_path+fileName ) ) / 86400 )
        # print( f"{fileName} age {fileAgeInDays}" )
        if fileAgeInDays > csv_retention_days:
            print(f"Deleting file: {fileName} age: {fileAgeInDays}")
            os.remove(csv_path+fileName)

def pushInfluxData():
    url = f"http://{confData['influxdb']['host']}:{confData['influxdb']['port']}"
    org = confData['influxdb']['organization']
    token = confData['influxdb']['token']
    bucket = confData['influxdb']['bucket']
    try:
        write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
    except:
        print(f"Influx DB error coulcn't connect to server using; URL {url}")
        return
    # We need to wrap this with some error handling
    # wind_direction_cp
    write_api = write_client.write_api(write_options=SYNCHRONOUS)
    idb_record = Point(f"weather_{project_name}")\
        .tag("cp16",sensorData['cp16'])\
        .field("temp_c",sensorData['temp_c'])\
        .field("humidity",sensorData['humidity'])\
        .field("batt_ok",sensorData['batt_ok'])\
        .field("wind_direction",sensorData['wind_direction'])\
        .field("wind_speed",sensorData['wind_speed'])\
        .field("max_wind_speed",sensorData['max_wind_speed'])\
        .field("rain_mm",sensorData['rain_mm'])\
        .field("rssi",sensorData['rssi'])
    write_api.write(bucket=bucket, org=org, record=idb_record)
    return


def printSensorData():
    now = datetime.datetime.now()
    nowTimeStamp = now.strftime("%Y-%m-%d %H:%M:%S")
    print("-------------------------------------")
    print(f"{nowTimeStamp}")
    print(f"Temperature = {sensorData['temp_c']}\u00B0C")
    print(f"Humidity = {sensorData['humidity']}%")
    print(f"Battery OK = {sensorData['batt_ok']}")
    print(f"Wind direction = {sensorData['wind_direction']}\u00B0")
    print(f"Wind direction CP16: {sensorData['cp16']}")
    print(f"Wind speed = {sensorData['wind_speed']} km/h")
    print(f"Max wind speed = {sensorData['max_wind_speed']} km/h")
    print(f"Rain = {sensorData['rain_mm']} mm")
    print(f"RSSI = {sensorData['rssi']} db")
    print("-------------------------------------")
    print("")



## Functions end
    
config_file = 'config.yaml'
if os.path.exists(config_file)==False:
    print(f"Missing config file expecting config file named: {config_file}")
    quit()

with open(config_file,"r") as fp_conf:
    confData=yaml.load(fp_conf,Loader=yaml.SafeLoader)
    print(f"MQTT Weather reading configuration file: {os.path.abspath(config_file)}")

project_name = confData['project']['name']

now = datetime.datetime.now()
nowTimeStamp = now.strftime("%Y-%m-%d %H:%M:%S")
nowDate = now.strftime("%Y-%m-%d")
sensorData = dict()

is_mqtt_enabled = str(confData['mqtt']['enabled']).lower() == 'true'
is_influxdb_enabled = str(confData['influxdb']['enabled']).lower() == 'true'
is_csv_enabled = str(confData['csv']['enabled']).lower() == 'true'

# Let MQTT try and get a connection while we are doing other processing
if is_mqtt_enabled:
    setupMqtt()
    # Loop and wait for MQTT messages *note this is the last thing we do....
    client.loop_forever()
