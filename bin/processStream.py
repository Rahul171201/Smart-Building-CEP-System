#!/usr/bin/python
# -*- coding: utf-8 -*-

import tkinter
from tkinter import *
import argparse
import json
import sys
import time
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException

def msg_process(msg):

    # Print the current time and the message.

    time_start = time.strftime('%Y-%m-%d %H:%M:%S')
    val = msg.value()
    dval = json.loads(val)
    #print (time_start, dval)
    res = []
    a = []
    for key, value in dval.items():
    	res.append(key)
    	a = dval[key]
    res.append(a)
    return res

def calc(temp, humid):
	top = tkinter.Tk()
	top.title('Aggregated data')
	frame1 = Frame(top)
	frame1.pack()
	l1 = Label(frame1, text='Average Temperature')
	l1.pack()
	messageVar1 = Message(frame1, text = str(temp))
	messageVar1.config(bg='lightgreen')
	messageVar1.pack( )
	frame2 = Frame(top)
	frame2.pack()
	l2 = Label(frame2, text='Average Humidity')
	l2.pack()
	messageVar2 = Message(frame2, text = str(humid))
	messageVar2.config(bg='lightgreen')
	messageVar2.pack( )
	top.mainloop()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')

    args = parser.parse_args()

    conf = {'bootstrap.servers': 'localhost:9092',
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'group.id': socket.gethostname()}

    consumer = Consumer(conf)

    running = True

    prev = 0
    count=0
    sum_temperature = 0.00
    avg_temperature = 0.00    
    sum_humidity = 0.00
    avg_humidity = 0.00    
    # button = tkinter.Button(top, text='TEMP', width=25, command=top.destroy)
    # button.pack()

    try:
        while running:
            consumer.subscribe([args.topic])

            msg = consumer.poll(1)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:

                    # End of partition event

                    sys.stderr.write('%% %s [%d] reached end at offset %d\n'
                             % (msg.topic(), msg.partition(),
                            msg.offset()))
                elif msg.error().code() \
                    == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    sys.stderr.write('Topic unknown, creating %s topic\n'
                             % args.topic)
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                w = msg_process(msg)
                #button = tkinter.Button(top, text=w, width=25,command=top.destroy)
                #button.pack()
                #calc_temp(w[0])
                hour = w[0][11:13]
                if(int(hour) != prev):
                	calc(avg_temperature, avg_humidity)
                	prev = int(hour)
                	avg_temperature = 0.00
                	sum_temperature = 0.00
                	avg_humidity = 0.00
                	sum_humidity = 0.00
                	count = 1
                	sum_temperature = sum_temperature + float(w[1][0])
                	sum_humidity = sum_humidity + float(w[1][1])
                else:
                	count = count + 1
                	sum_temperature = sum_temperature + float(w[1][0])
                	avg_temperature = ((sum_temperature) / (count))
                	sum_humidity = sum_humidity + float(w[1][1])
                	avg_humidity = ((sum_humidity) / (count))
                print(w[0][11:13])
    except KeyboardInterrupt:

        pass
    finally:

        # Close down consumer to commit final offsets.

        consumer.close()

    #top.mainloop()


if __name__ == '__main__':
    main()

