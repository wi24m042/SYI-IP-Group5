FROM ubuntu:latest
RUN apt-get update && apt-get upgrade -y
RUN apt-get install tzdata -y
RUN apt-get install python3-pip -y
RUN apt-get install python3.12-venv -y
RUN apt-get install git -y
RUN git clone https://github.com/arskom/spyne.git
RUN python3 -m venv fhtw-venv
RUN fhtw-venv/bin/pip3 install pandas influxdb_client aiohttp-spyne aiohttp_cors build lxml
RUN fhtw-venv/bin/pip3 uninstall spyne -y
WORKDIR "/spyne"
RUN /fhtw-venv/bin/python3 -m build
WORKDIR "/"
RUN fhtw-venv/bin/pip3 install spyne/dist/spyne-2.15.0a0-py3-none-any.whl
CMD fhtw-venv/bin/python3 api-provider.py > /iss-tracker.log 2>&1