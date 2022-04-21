#!/usr/bin/env python

import argparse
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('filename', type=str,
                        help='Time series csv file.')
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    topic = args.topic
    p_key = args.filename

    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)
    
    '''humidity_average = rdr["Humidity"].mean()
    temp_average = rdr["Temperature"].mean()
    light_average = rdr["Light"].mean()
    co2_average = rdr["CO2"].mean()
    humidityratio_average = rdr["HumidityRatio"].mean()
    occupancy_average = rdr["Occupancy"].mean()'''

    rdr = csv.reader(open(args.filename))
    next(rdr)  # Skip header
    firstline = True

    while True:

        try:

            if firstline is True:
                line1 = next(rdr, None)
                timestamp= line1[0]
                
                value = []
                for i in range(1,7):
                	value.append(float(line1[i]));
                
                # Convert csv columns to key value pair
                result = {}
                result[timestamp] = value
                # Convert dict to json as message format
                jresult = json.dumps(result)
                firstline = False

                producer.produce(topic, key=p_key, value=jresult, callback=acked)

            else:
                line = next(rdr, None)
                d1 = parse(timestamp)
                d2 = parse(line[0])
                diff = ((d2 - d1).total_seconds())/args.speed
                time.sleep(diff)
                timestamp = line[0]
                
                value = []
                for i in range(1,7):
                	value.append(line[i]);
                
                result = {}
                result[timestamp] = value
                jresult = json.dumps(result)

                producer.produce(topic, key=p_key, value=jresult, callback=acked)

            producer.flush()

        except TypeError:
            sys.exit()


if __name__ == "__main__":
    main()
