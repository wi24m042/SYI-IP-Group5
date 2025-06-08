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
    """
    Custom Spyne application for the Position History Service.
    """

    def __init__(self, services:list, tns:str, in_protocol:Soap12, out_protocol:Soap12, influx_reader:'InfluxDBReader'):
        """
        Initializes the PositionHistoryServiceApplication as a custom SpyneApplication.

        Parameters:
            self
            services (list): List of services to be included in the application.
            tns (str): Target namespace for the application.
            in_protocol (Soap12): Input protocol for the application.
            out_protocol (Soap12): Output protocol for the application.
            influx_reader (InfluxDBReader): InfluxDB reader instance.
        """
        super().__init__(services=services, tns=tns, in_protocol=in_protocol, out_protocol=out_protocol)
        self.influx_reader = influx_reader

class LocationRecord(ComplexModel):
    """
    This custom Spyne ComplexModel represents a location record with timestamp, latitude, longitude, and source.
    """
    
    timestamp = Integer
    latitude = Float
    longitude = Float
    source = Unicode

class PositionHistoryService(ServiceBase):
    """
    This service provides methods to retrieve location history and closest entries by timestamp.
    """

    @rpc(Integer, Integer, _returns=Iterable(LocationRecord))
    def GetLocationHistory(ctx, start_time:int, stop_time:int) -> Generator[LocationRecord, None, None] | None:
        """
        API function to retrieve location history records between the specified start and stop times from the InfluxDB.

        Parameters:
            ctx: The context of the request.
            start_time (int): The start time in seconds since epoch.
            stop_time (int): The stop time in seconds since epoch.

        Returns:
            json_list (Generator | None): A generator yielding LocationRecord objects or None if an error occurs.
        """
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
        """
        API function to retrieve the closest location entry by timestamp from the InfluxDB.

        Parameters:
            ctx: The context of the request.
            timestamp (int): The timestamp in seconds since epoch.

        Returns:
            location_record (LocationRecord | None): The closest location record or None if not found.
        """
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
    """
        This class is responsible for querying the InfluxDB database for location data.
    """
    def __init__(self, url:str, token:str, org:str, bucket:str, measurement:str, source:str, ca_cert:str):
        """
        Initializes the InfluxDBReader with the necessary parameters to connect to the InfluxDB.

        Parameters:
            url (str): The URL of the InfluxDB instance.
            token (str): The authentication token for the InfluxDB.
            org (str): The organization name in the InfluxDB.
            bucket (str): The bucket name in the InfluxDB.
            measurement (str): The measurement name to query.
            source (str): The source of the data to filter by.
            ca_cert (str): Path to the CA certificate for SSL verification.
        """
        self.client = InfluxDBClient(url=url, token=token, org=org, ssl_ca_cert=ca_cert, verify_ssl=True)
        self.query_api = self.client.query_api()
        self.bucket = bucket
        self.org = org
        self.measurement = measurement
        self.source = source

    def query_builder(self, start_time:int, stop_time:int) -> str:
        """
        Builds a Flux query string to retrieve location data from the InfluxDB.

        Parameters:
            start_time (int): The start time in seconds since epoch.
            stop_time (int): The stop time in seconds since epoch.

        Returns:    
            query (str): The Flux query as a string.
        """
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
        """
        Executes the provided Flux query against the InfluxDB and returns the results.

        Parameters:
            query (str): The Flux query to execute.

        Returns:
            table_list (TableList): The resulting entries of the query from the InfluxDB as a TableList.
        """
        try:
            table_list = self.query_api.query(org=self.org, query=query)
            logging.info(f"Query executed successfully: {query}")
            return table_list
        except Exception as e:
            logging.error(f"Failed to execute query: {e}")
            return None
        
    def execute_read_request(self, start_time:int, stop_time:int) -> TableList:
        """
        Executes a read request to the InfluxDB for location data between the specified start and stop times.

        Parameters:
            start_time (int): The start time in seconds since epoch.
            stop_time (int): The stop time in seconds since epoch.

        Returns:    
            table_list (TableList): The resulting entries of the query from the InfluxDB as a TableList.
        """
        return self.read_data(self.query_builder(start_time, stop_time))

class Main:
    """
    Main class to provide a SOAP service via an HTTPS server.
    """
    
    def __init__(self):
        """
        Initializes the Main class, reads the configuration, sets up logging, creates InfluxDB reader, creates SOAP service, creates server instance, and sets up SSL context.
        """
        self.read_config()
        self.setup_logging()
        self.create_influxdb_reader_instance()
        self.create_soap_service_instance()
        self.create_server_instance()
        self.create_ssl_context()
    
    def read_config(self):
        """
        Function to read the configuration from the config.ini file.

        Parameters:
            self
        """
        self.config = ConfigParser()
        self.config.read('config.ini')
    
    def setup_logging(self):
        """
        Function to set up logging configuration.

        Parameters:
            self
        """
        logging.basicConfig(
            filename=self.config.get('logging', 'log_file'),
            level=logging.INFO,
            format='%(asctime)s %(levelname)s: %(message)s'
        )
        logging.info("Logging setup complete")

    def create_influxdb_reader_instance(self):
        """
        Function to create an instance of the InfluxDBReader class.

        Parameters:
            self
        """
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
        """
        Function to create an instance of the PositionHistoryServiceApplication class which is a customized SpyneApplication.

        Parameters:
            self
        """
        self.phs_application = PositionHistoryServiceApplication(
            services=[PositionHistoryService],
            tns=self.config.get('api-provider', 'namespace'),
            in_protocol=Soap12(validator='lxml'),
            out_protocol=Soap12(),
            influx_reader=self.influx_reader
        )
        logging.info("PositionHistoryServiceApplication instance created")

    def create_server_instance(self):
        """
        Function to create an instance of the AIOSpyne handler and set up the web application with CORS.

        Parameters:
            self
        """
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
        """
        Function to create an SSL context for secure communication.

        Parameters:
            self
        """
        self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.ssl_context.load_cert_chain(certfile=self.config.get("api-provider", "server_cert"), keyfile=self.config.get("api-provider", "server_key"))
        logging.info("SSL context created")

    def server_loop(self):
        """
        Function to start the server loop for the SOAP service web application.
        """
        logging.info("Starting server loop")
        web.run_app(self.phs_web_app, port=self.config.getint('api-provider', 'port'), ssl_context=self.ssl_context)

if __name__ == "__main__":
    main = Main()
    main.server_loop()