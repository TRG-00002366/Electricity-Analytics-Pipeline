# Electricity-Analytics-Pipeline
This project will be worked on by Justin Evans and Robert Martin



Instructions for running the project

Make sure you have docker installed on your system
Use the link below to arrive at docker website where you can install docker if you do not already have it
https://www.docker.com/get-started/

use the following command while in the code folder:
docker-compose up -d

you can also use the following command to turn the docker off
docker-compose down
or
docker-compose down -v
if you include the -v, make sure to run the following command as well from the code folder
This is needed since the offsets
rm -rf data
