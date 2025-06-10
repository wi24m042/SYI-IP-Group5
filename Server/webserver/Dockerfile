FROM ubuntu:latest
RUN apt-get update && apt-get upgrade -y
RUN apt-get install tzdata -y
RUN apt-get install python3-pip -y
RUN apt-get install python3.12-venv -y
RUN python3 -m venv fhtw-venv
RUN fhtw-venv/bin/pip3 install zeep jsonschema requests aiohttp
CMD fhtw-venv/bin/python3 webserver.py > /iss-tracker.log 2>&1