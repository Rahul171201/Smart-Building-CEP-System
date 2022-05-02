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
global top
top = tkinter.Tk()
top.geometry("600x400")
top["bg"] = '#DCEDC2'
top.title('Aggregated data')
global frame1
frame1 = Frame(top, width = 70)
frame1.config(bg = '#A8E6CE')
frame1.pack(padx = 20, pady = 20)
#frame1.grid(column = 1, row = 1)
global l1
l1 = Label(frame1, text='Average Temperature')
l1.config(bg = '#A8E6CE')
l1.pack(padx = 5, pady = 5)
global messageVar1
messageVar1 = Message(frame1, text = "")
messageVar1.config(bg='#A8E6CE')
messageVar1.pack(padx = 5, pady = 5)
global frame2
frame2 = Frame(top,width = 70)
frame2.config(bg = '#A8E6CE')
frame2.pack(padx = 20, pady = 20)
#frame2.grid(column = 2, row = 1)
global l2
l2 = Label(frame2, text='Average Humidity')
l2.config(bg = '#A8E6CE')
l2.pack(padx = 5, pady = 5)
global messageVar2
messageVar2 = Message(frame2, text = "")
messageVar2.config(bg='#A8E6CE')
messageVar2.pack(padx = 5,pady = 5)
#utton = tkinter.Button(top, text='TEMP', width=25, command=top.update())
#button.pack()

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
	'''top = tkinter.Tk()
	top.title('Aggregated data')
	frame1 = Frame(top)
	frame1.pack()
	l1 = Label(frame1, text='Average Temperature')
	l1.pack()'''
	#messageVar1 = Message(frame1, text = str(temp))
	messageVar1.config(text = str(temp) + "   " ,bg='lightgreen')
	'''messageVar1.pack( )
	frame2 = Frame(top)
	frame2.pack()
	l2 = Label(frame2, text='Average Humidity')
	l2.pack()'''
	#messageVar2 = Message(frame2, text = str(humid))
	messageVar2.config(text = str(humid) + "   ",bg='lightgreen')
	top.update()
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

    #top.mainloop()
 
	
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
                	calc(round(avg_temperature,3),round(avg_humidity,3))
                	#top.update()
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
    #top.mainloop()
    except KeyboardInterrupt:

        pass
    finally:

        # Close down consumer to commit final offsets.

        consumer.close()

    


if __name__ == '__main__':
    main()

top.mainloop()
