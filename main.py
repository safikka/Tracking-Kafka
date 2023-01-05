import logging
import serial
import re
import time
import kafka
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Read the configuration file
with open('config.json', 'r') as f:
    config = json.load(f)

# Get the serial port, Kafka server, and Kafka topic from the config file
serial_port = config['serial_port']
baudrate = config['baudrate']
kafka_server = config['kafka_server']
kafka_topic = config['kafka_topic']
duration = config['duration_send_in_second']
mode_data = config['mode_data']

# Open the serial port if mode_data == 1
if mode_data == 1 :
    ser = serial.Serial(serial_port, baudrate)

# Connect to the Kafka broker
producer = kafka.KafkaProducer(bootstrap_servers=kafka_server)

# Test count
count = 1

while True:

    if mode_data == 1:

        # Info mode Serial
        logger.info('Data from Serial')

        # Read a line of data from the serial port
        line = ser.readline().decode('utf-8').strip()
        logger.info('Read line from serial port: %s', line)

        # Check if the line is a valid NMEA sentence
        if not re.match(r'^\$GPGGA', line):
            logger.info('Skipping invalid NMEA sentence')
            continue

        # Split the line into fields
        fields = line.split(',')
        logger.info('Split line into fields: %s', fields)

        # Parse the latitude, longitude, and number of satellites
        latitude = float(fields[2])
        longitude = float(fields[4])
        num_sats = int(fields[7])
        logger.info('Parsed latitude, longitude, and num_sats: %f, %f, %d', latitude, longitude, num_sats)

        # Convert latitude and longitude to decimal degrees
        # (if the coordinates are in degrees, minutes, and seconds)
        if fields[3] == 'S':
            latitude = -latitude
        if fields[5] == 'W':
            longitude = -longitude
        latitude = latitude / 100 + (latitude % 100) / 60
        longitude = longitude / 100 + (longitude % 100) / 60
        logger.info('Converted lat/long to decimal degrees: %f, %f', latitude, longitude)

        # # Parse the speed (if available)
        # speed = None
        # if fields[8]:
        #     speed = float(fields[8])
        #     logger.info('Parsed speed: %f', speed)

        # # Parse the GPS time (if available)
        # gps_time = None
        # if fields[1]:
        #     gps_time = time.strptime(fields[1], '%H%M%S.%f')
        #     logger.info('Parsed GPS time: %s', gps_time)

        # # Assemble the data into a dictionary
        # data = {
        #     'latitude': latitude,
        #     'longitude': longitude,
        #     'num_sats': num_sats,
        #     'speed': speed,
        #     'gps_time': gps_time,
        # }
        # logger.info('Assembled data: %s', data)

        # Assemble the data into a dictionary
        data = {
            'latitude': latitude,
            'longitude': longitude,
            'num_sats': num_sats
        }
        logger.info('Assembled data: %s', data)

        # Send the data to the Kafka broker
        producer.send(kafka_topic, data)
        logger.info('Sent data to Kafka broker')
        time.sleep(duration)

    else:

        # Info mode Serial
        logger.info('Data from Dummy')

        data = "{\"Hell\": " + str(count) + "}"
        producer.send(kafka_topic, data.encode('utf-8'))
        count += 1
        logger.info('Sent data to Kafka broker')
        time.sleep(duration)

producer.close()