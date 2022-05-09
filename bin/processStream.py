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
from tkinter import ttk
from tkinter.font import Font
global window
window = Tk()
bg = PhotoImage(file = "new.png")
# Show image using label
Ft = ("Comic Sans MS", 20)
Ft1 = ("Comic Sans MS", 24)
label1 = Label( window, image = bg)
label1.place(x = 0, y = 0,relheight=1,relwidth=1)
window.grid_columnconfigure(0,weight=1)
window.title('Plotting in Tkinter')
mainLabel = Label(window, text= "CEP Monitoring System", font = Ft1,bg = '#382E50',fg = '#E8FFAC' )
mainLabel.grid(row=0,column = 1,sticky  = "nsew",padx = 3, pady = 40)
frame1 = Frame(window,height = 20,width = 30,padx=15, pady=15)
frame1.config(bg = '#FF7986')
#frame1.grid(row=0,column = 0)
l1 = Label(frame1,text='Average \nTemperature', font = Ft)
l1.config(bg = '#FF7986')
l1.pack(padx = 1, pady = 1)
m1 = Message(frame1, text = "temp    df", relief=SUNKEN,font = Ft,fg = '#E8FFAC')
m1.config(bg='#65426D')
m1.pack(padx = 1, pady = 1)
#pw.add(frame1)
##############################################
frame2 = Frame(window,height = 20,width = 30,padx=15, pady=15)
frame2.config(bg = '#FF7986')
l2 = Label(frame2, text='Average Humidity\n',font = Ft)
l2.config(bg = '#FF7986')
l2.pack(padx = 1, pady = 1)
m2 = Message(frame2, text = "humid ",relief=SUNKEN,font = Ft,fg = '#E8FFAC')
m2.config(bg='#65426D')
m2.pack(padx = 1,pady = 1)
##############################################
frame3 = Frame(window,height = 20,width =30,padx=15, pady=15)
frame3.config(bg = '#FF7986')
l3 = Label(frame3, text='Average CO2\n',font = Ft)
l3.config(bg = '#FF7986')
l3.pack(padx = 1, pady = 1)
m3 = Message(frame3, text = "Co2",relief=SUNKEN,font = Ft,fg = '#E8FFAC')
m3.config(bg='#65426D')
m3.pack(padx = 1,pady = 1)
##############################################
frame4 = Frame(window,height = 20,width = 30,padx=15, pady=15)
frame4.config(bg = '#FF7986')
l4 = Label(frame4, text='Average \nHumidity Ratio',font = Ft)
l4.config(bg = '#FF7986')
l4.pack(padx = 1, pady = 1)
global m4
m4 = Message(frame4, text = "humid ratio",relief=SUNKEN,font = Ft,fg = '#E8FFAC')
m4.config(bg='#65426D')
m4.pack(padx = 1,pady = 1)
##############################################
global frame5
frame5 = Frame(window,height = 20, width = 30,padx=15, pady=15)
frame5.config(bg = '#FF7986')
global l5
l5 = Label(frame5, text='Light\n',font = Ft)
l5.config(bg = '#FF7986')
l5.pack(padx = 1, pady = 1)
global m5
m5 = Message(frame5, text = "light",relief=SUNKEN,font = Ft,fg = '#E8FFAC')
m5.config(bg='#65426D')
m5.pack(padx = 1,pady = 1)
##############################################
global frame6
frame6 = Frame(window,height = 20,width = 30, relief=RAISED,padx=15, pady=15)
frame6.config(bg = '#FF7986')
global l6
l6 = Label(frame6, text='Occupancy\n',font = Ft)
l6.config(bg = '#FF7986')
l6.pack(padx = 1, pady = 1)
global m6
m6 = Message(frame6, text = "occupancy",relief=SUNKEN,font = Ft,fg = '#E8FFAC')
m6.config(bg='#65426D')
m6.pack(padx = 1,pady = 1)
##############################################
frame1.grid(row=1,column = 0,sticky  = "nsew",padx = 3, pady = 10)
frame2.grid(row=1,column = 1,sticky  = "nsew",padx = 3, pady = 10)
frame3.grid(row=1,column = 2,sticky  = "nsew",padx = 3, pady = 10)
frame4.grid(row=2,column = 0,sticky  = "nsew",padx = 3, pady = 10)
frame5.grid(row=2,column = 1,sticky  = "nsew",padx = 3, pady = 10)
frame6.grid(row=2,column = 2,sticky  = "nsew",padx = 3, pady = 10)
#window.mainloop()

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

def calc(temp, humid, CO2, HR, light, occupancy):
	'''top = tkinter.Tk()
	top.title('Aggregated data')
	frame1 = Frame(top)
	frame1.pack()
	l1 = Label(frame1, text='Average Temperature')
	l1.pack()'''
	#messageVar1 = Message(frame1, text = str(temp))
	m1.config(text = str(temp) + "   °C")
	'''messageVar1.pack( )
	frame2 = Frame(top)
	frame2.pack()
	l2 = Label(frame2, text='Average Humidity')
	l2.pack()'''
	#messageVar2 = Message(frame2, text = str(humid))
	m2.config(text = str(humid) + "   g/kg")
	m3.config(text = str(CO2) + "   in ppm")
	m4.config(text = str(HR) + "   ")
	light = int(light)
	occupancy = int(occupancy)
	if(light>0):
		m5.config(text = str(light) + "   lux",bg='red')
	else:
		m5.config(text = str(light) + "   lux",bg='#65426D')
	if(occupancy>0):
		m6.config(text = str(occupancy) + "   ",bg = 'red')
	else:
		m6.config(text = str(occupancy) + "   ",bg='#65426D')
	window.update()
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
    sum_CO2 = 0.00
    avg_CO2 = 0.00    
    sum_humidityRatio = 0.00
    avg_humidityRatio = 0.00 
    light = 0
    occupancy = 0   
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
                	calc(round(avg_temperature,2),round(avg_humidity,2),round(avg_CO2,2), round(avg_humidityRatio*100,2), light, occupancy)
                	#top.update()
                	prev = int(hour)
                	avg_temperature = 0.00
                	sum_temperature = 0.00
                	avg_humidity = 0.00
                	sum_humidity = 0.00
                	avg_CO2 = 0.00
                	sum_CO2 = 0.00
                	avg_humidityRatio = 0.00
                	sum_humidityRatio = 0.00
                	count = 1
                	sum_temperature = sum_temperature + float(w[1][0])
                	sum_humidity = sum_humidity + float(w[1][1])
                	sum_CO2 = sum_CO2 + float(w[1][3])
                	sum_humidityRatio = sum_humidityRatio + float(w[1][4])
                else:
                	count = count + 1
                	sum_temperature = sum_temperature + float(w[1][0])
                	avg_temperature = ((sum_temperature) / (count))
                	sum_humidity = sum_humidity + float(w[1][1])
                	avg_humidity = ((sum_humidity) / (count))
                	sum_CO2 = sum_CO2 + float(w[1][3])
                	avg_CO2 = ((sum_CO2) / (count))
                	sum_humidityRatio = sum_humidityRatio + float(w[1][4])
                	avg_humidityRatio = ((sum_humidityRatio) / (count))
                	light = w[1][2]
                	occupancy = w[1][5]
                print(w[0][11:13])
    #top.mainloop()
    except KeyboardInterrupt:

        pass
    finally:

        # Close down consumer to commit final offsets.

        consumer.close()

    


if __name__ == '__main__':
    main()

window.mainloop()
