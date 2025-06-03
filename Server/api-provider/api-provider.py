import ssl
import logging
import pandas as pd
from typing import Generator
from configparser import ConfigParser
from influxdb_client import InfluxDBClient
from influxdb_client.client.flux_table import TableList
from aiohttp import web
import aiohttp_cors
from aiohttp_spyne import AIOSpyne
from aiohttp.web_response import Response
from spyne.protocol.soap import Soap12
from spyne import Application as SpyneApplication, rpc, ServiceBase, Iterable, Integer, Unicode, ComplexModel, Float

class PositionHistoryServiceApplication(SpyneApplication):
    def __init__(self, services, tns, in_protocol, out_protocol, influx_reader):
        super().__init__(services=services, tns=tns, in_protocol=in_protocol, out_protocol=out_protocol)
        self.influx_reader = influx_reader

class LocationRecord(ComplexModel):
    timestamp = Integer
    latitude = Float
    longitude = Float
    source = Unicode

class PositionHistoryService(ServiceBase):
    @rpc(Integer, Integer, _returns=Iterable(LocationRecord))
    def GetLocationHistory(ctx, start_time:int, stop_time:int) -> Generator[LocationRecord, None, None] | None:
        logging.info(f"Received request for location history between {start_time} and {stop_time}")
        table_list = ctx.app.influx_reader.execute_read_request(start_time=start_time, stop_time=stop_time)
        if table_list is not None:
            for table in table_list:
                for record in table.records:
                    location_record = LocationRecord(
                        timestamp=int(record.get_time().timestamp()),
                        latitude=record.values.get("latitude"),
                        longitude=record.values.get("longitude"),
                        source=record.values.get("source")
                    )
                    logging.info(f"Yielding location record: {location_record}")
                    yield location_record
        else:
            logging.error("Failed to retrieve data from InfluxDB")
            yield None

    @rpc(Integer, _returns=LocationRecord)
    def GetClosestEntryByTimestamp(ctx, timestamp:int) -> LocationRecord | None:
        logging.info(f"Received request for closest entry by timestamp {timestamp}")
        start_time = timestamp - 60
        stop_time = timestamp + 60

        while True:
            logging.info(f"Querying data from {start_time} to {stop_time}")
            table_list = ctx.app.influx_reader.execute_read_request(start_time=start_time, stop_time=stop_time)

            if table_list is not None:
                data_points = [record.values for table in table_list for record in table.records] # list of dicts
                logging.info(f"Found {len(data_points)} data points in the range {start_time} to {stop_time}")

                if len(data_points) == 0:
                    start_time -= 60
                    stop_time += 60
                    if start_time < timestamp - 3600:
                        logging.info(f"Expanded range to {start_time} to {stop_time}, but still no data points found")
                        return None
                    logging.info(f"No data points found, expanding range to {start_time} to {stop_time}")
                else:
                    points_df = pd.DataFrame(data_points)
                    logging.info(f"Data points DataFrame:\n{points_df}")
                    points_df['_time'] = pd.to_datetime(points_df['_time'])
                    points_df['delta'] = abs(points_df['_time'] - pd.to_datetime(timestamp, unit='s', utc=True))
                    closest = points_df.loc[points_df['delta'].idxmin()]
                    logging.info(f"Closest entry found: {closest}")
                    location_record = LocationRecord(
                        timestamp=int(closest.get("_time").timestamp()),
                        latitude=float(closest.get("latitude")),
                        longitude=float(closest.get("longitude")),
                        source=closest.get("source")
                    )
                    logging.info(f"Returning closest entry: {location_record}")
                    return location_record
            else:
                logging.error("Failed to retrieve data from InfluxDB")
                return None

class InfluxDBReader:
    def __init__(self, url:str, token:str, org:str, bucket:str, measurement:str, source:str, ca_cert:str):
        self.client = InfluxDBClient(url=url, token=token, org=org, ssl_ca_cert=ca_cert, verify_ssl=True)
        self.query_api = self.client.query_api()
        self.bucket = bucket
        self.org = org
        self.measurement = measurement
        self.source = source

    def query_builder(self, start_time:int, stop_time:int) -> str:
        query = f'''
                from(bucket: "{self.bucket}")
                    |> range(start: {start_time}, stop: {stop_time})
                    |> filter(fn: (r) => r._measurement == "{self.measurement}")
                    |> filter(fn: (r) => r.source == "{self.source}")
                    |> filter(fn: (r) => r._field == "latitude" or r._field == "longitude")
                    |> pivot(
                        rowKey: ["_time"],
                        columnKey: ["_field"],
                        valueColumn: "_value"
                    )
                '''
        return query
    
    def read_data(self, query:str) -> TableList:
        try:
            table_list = self.query_api.query(org=self.org, query=query)
            logging.info(f"Query executed successfully: {query}")
            return table_list
        except Exception as e:
            logging.error(f"Failed to execute query: {e}")
            return None
        
    def execute_read_request(self, start_time:int, stop_time:int) -> TableList:
        return self.read_data(self.query_builder(start_time, stop_time))

class Main:
    def __init__(self):
        self.read_config()
        self.setup_logging()
        self.create_influxdb_reader_instance()
        self.create_soap_service_instance()
        self.create_server_instance()
        self.create_ssl_context()
    
    def read_config(self):
        self.config = ConfigParser()
        self.config.read('config.ini')
    
    def setup_logging(self):
        logging.basicConfig(
            filename=self.config.get('logging', 'log_file'),
            level=logging.INFO,
            format='%(asctime)s %(levelname)s: %(message)s'
        )
        logging.info("Logging setup complete")

    def create_influxdb_reader_instance(self):
        self.influx_reader = InfluxDBReader(
            url=self.config.get('influxdb', 'url'),
            token=self.config.get('influxdb', 'token'),
            org=self.config.get('influxdb', 'org'),
            bucket=self.config.get('influxdb', 'bucket'),
            measurement=self.config.get('influxdb', 'measurement'),
            source=self.config.get('iss-api', 'source'),
            ca_cert=self.config.get('influxdb', 'ca_cert')
        )
        logging.info("InfluxDB Reader instance created")

    def create_soap_service_instance(self):
        self.phs_application = PositionHistoryServiceApplication(
            services=[PositionHistoryService],
            tns=self.config.get('api-provider', 'namespace'),
            in_protocol=Soap12(validator='lxml'),
            out_protocol=Soap12(),
            influx_reader=self.influx_reader
        )
        logging.info("PositionHistoryServiceApplication instance created")

    def create_server_instance(self):
        self.phs_handler = AIOSpyne(self.phs_application)
        self.phs_web_app = web.Application()
        get_route = self.phs_web_app.router.add_get('/{tail:.*}', self.phs_handler.get)
        post_route = self.phs_web_app.router.add_post('/{tail:.*}', self.phs_handler.post)
        cors = aiohttp_cors.setup(
            self.phs_web_app,
            defaults =
            {
                "https://ntgddns.asuscomm.com": aiohttp_cors.ResourceOptions(
                    allow_credentials=True,
                    expose_headers="*",
                    allow_headers="*",
                )
            }
        )
        
        cors.add(get_route)
        cors.add(post_route)
        logging.info("Server instance created")

    def create_ssl_context(self):
        self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.ssl_context.load_cert_chain(certfile=self.config.get("api-provider", "server_cert"), keyfile=self.config.get("api-provider", "server_key"))
        logging.info("SSL context created")

    def server_loop(self):
        logging.info("Starting server loop")
        web.run_app(self.phs_web_app, port=self.config.getint('api-provider', 'port'), ssl_context=self.ssl_context)

if __name__ == "__main__":
    main = Main()
    main.server_loop()