# Smart-Building-CEP-System
A CEP System for smart building monitoring using Apache Flink and Kafka

# Technology and Resources used
Python is used as the programming language. The data streaming is carried out with the help of Apache kafka. The containers are managed and run by docker. The dataset has been taken from kaggle. The dataset contains statistics of CO2 level, occupancy, light and more inside a Smart building. The data is taken with the help of sensors installed within the building.

# Algorithm
The dataset is firstly preprocessed and fed into the stream. We get real time data stream from the producer which we can read and further process through the consumer. The data is further analyzed and displayed through out graphical user interface made with tkinter.

# Steps to run the project
Clone the repository and go into the respective folder. 
Then firstly start the zookeeper and kafka servers.
```
sudo docker compose-up
```
Once the servers are up and running, cd to bin folder.
```
cd bin
```
Then start the consumer first to listen to the incoming data stream. (creating a kafka topic named 'my-stream')
```
python3 processStream.py my-stream
```
Start the producer to start sending data into the kafka stream 'my-stream' with speed of 100
```
python3 sendStream.py data.csv my-stream --speed 100
```
On starting the producer, you will be prompted with a login screen

![Screenshot from 2022-09-30 19-25-28](https://user-images.githubusercontent.com/70642284/193324041-38f86eba-5ddb-4949-ad27-802928dada93.png)

Enter the required credentials to start the producer.
Username :
```
rahul
```
Password :
```
123
```
The producer will then start sending data at particular intervals of time (with a specific latency) and the consumer will read data and further process and present to us through the user interface. 

![Screenshot from 2022-09-30 19-32-33](https://user-images.githubusercontent.com/70642284/193324092-fac18895-ab05-4572-b4ec-1bd576090313.png)


Hence we get to look at real time data.
