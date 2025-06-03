import os
import ssl
import json
import logging
from aiohttp import web
from configparser import ConfigParser
from zeep import Client
from zeep.transports import Transport
from zeep.helpers import serialize_object
import requests
from jsonschema import validate, ValidationError

class SoapClient:
    def __init__(self, wsdl_url:str):
        self.wsdl_url = wsdl_url
        self.create_soap_client()

    def create_soap_client(self):
        self.session = requests.Session()
        try:
            self.soap_client = Client(wsdl=self.wsdl_url, transport=Transport(session=self.session))
            logging.info(f"SOAP client created with WSDL URL: {self.wsdl_url}")
        except Exception as e:
            logging.error(f"Error creating SOAP client: {e}")
            raise

    def get_location_history(self, start_time:int, stop_time:int) -> list | None:
        try:
            logging.info(f"Fetching location history from {start_time} to {stop_time}")
            response = self.soap_client.service.GetLocationHistory(start_time, stop_time)
            logging.info(f"Location history fetched successfully: {response}")
            return response
        except Exception as e:
            logging.error(f"Error fetching location history: {e}")
            return None
        
    def get_closest_entry_by_timestamp(self, timestamp:int) -> list | None:
        try:
            logging.info(f"Fetching closest entry by timestamp: {timestamp}")
            response = self.soap_client.service.GetClosestEntryByTimestamp(timestamp=timestamp)
            logging.info(f"Closest entry fetched successfully: {response}")
            return [response]
        except Exception as e:
            logging.error(f"Error fetching closest entry by timestamp: {e}")
            return None

class ServerBackend:
    def __init__(self, soap_client:SoapClient, schema_filepath:str):
        self.soap_client = soap_client
        self.json_schema = self.load_json_schema(schema_filepath)
        self.base_dir = os.path.dirname(os.path.abspath(__file__))

    def load_json_schema(self, schema_filepath: str) -> dict:
        with open(schema_filepath, "r") as file:
            return json.load(file)

    def validate_json(self, json_content:dict) -> bool:
        try:
            validate(instance=json_content, schema=self.json_schema)
            logging.info("API request is valid according to the JSON schema.")
            return True
        except ValidationError as e:
            logging.error(f"JSON schema validation failed: {e}")
            return False
            
    async def handle_index(self, request:web.Request) -> web.FileResponse:
        return web.FileResponse("html/index.html")
    
    async def handle_favicon(self, request:web.Request) -> web.FileResponse:
        return web.FileResponse("html/resources/PHS_favicon.png")
    
    async def handle_PHSicon(self, request:web.Request) -> web.FileResponse:
        return web.FileResponse("html/resources/PHS_icon-small.png")

    async def handle_leaflet_js(self, request:web.Request) -> web.FileResponse:
        return web.FileResponse("html/resources/leaflet.icon-material.js")

    async def handle_leaflet_css(self, request:web.Request) -> web.FileResponse:
        return web.FileResponse("html/resources/leaflet.icon-material.css")

    async def handle_get_location_history(self, request:web.Request) -> web.Response:
        try:
            rest_data = await request.json()
            logging.info(f"Received request data: {rest_data}")
            if self.validate_json(rest_data):
                start_time = rest_data.get("PositionHistoryService", {}).get("GetLocationHistory", {}).get("StartTime")
                stop_time = rest_data.get("PositionHistoryService", {}).get("GetLocationHistory", {}).get("StopTime")
                response = self.soap_client.get_location_history(start_time, stop_time)
                if response is None:
                    response = []
                return web.json_response(response, dumps=lambda obj: json.dumps(serialize_object(obj), indent=2))
            else:
                logging.error("Invalid JSON schema")
                return web.json_response([], status=400)
        except Exception as e:
            logging.error(f"Internal error: {e}")
            data = await request.post()
        return web.json_response({"received": data})
    
    async def handle_get_closest_entry_by_timestamp(self, request:web.Request) -> web.Response:
        try:
            rest_data = await request.json()
            logging.info(f"Received request data: {rest_data}")
            if self.validate_json(rest_data):
                timestamp = rest_data.get("PositionHistoryService", {}).get("GetClosestEntryByTimestamp", {}).get("Timestamp")
                response = self.soap_client.get_closest_entry_by_timestamp(timestamp)
                return web.json_response(response, dumps=lambda obj: json.dumps(serialize_object(obj), indent=2))
            else:
                logging.error("Invalid JSON schema")
                return web.json_response([], status=400)
        except Exception as e:
            logging.error(f"Internal error: {e}")
            data = await request.post()
        return web.json_response({"received": data})

    async def handle_404(self, request:web.Request) -> web.Response:
        return web.Response(status=404, text=f"404 not found: {request.path}")
    
class Main:
    def __init__(self):
        self.read_config()
        self.setup_logging()
        self.create_soap_client()
        self.create_backend()
        self.create_server_instance()
        self.create_ssl_context()
    
    def read_config(self):
        self.config = ConfigParser()
        self.config.read("config.ini")
    
    def setup_logging(self):
        logging.basicConfig(
            filename=self.config.get("logging", "log_file"),
            level=logging.INFO,
            format="%(asctime)s %(levelname)s: %(message)s"
        )
        logging.info("Welcome to the Position History Service (PHS) web server - the current time is %s",)
        logging.info("Logging setup complete")

    def create_soap_client(self):
        self.soap_client = SoapClient(self.config.get("soap", "wsdl_url"))

    def create_backend(self):
        self.backend = ServerBackend(self.soap_client, self.config.get("rest-api", "rest_api_schema"))
        logging.info("Server backend created")

    def create_server_instance(self):
        self.web_app = web.Application()
        self.web_app.router.add_get("/", self.backend.handle_index)
        self.web_app.router.add_get("/index.html", self.backend.handle_index)
        self.web_app.router.add_get("/resources/PHS_favicon.png", self.backend.handle_favicon)
        self.web_app.router.add_get("/resources/PHS_icon-small.png", self.backend.handle_PHSicon)
        self.web_app.router.add_get("/resources/leaflet.icon-material.js", self.backend.handle_leaflet_js)
        self.web_app.router.add_get("/resources/leaflet.icon-material.css", self.backend.handle_leaflet_css)
        self.web_app.router.add_post("/api/get_location_history", self.backend.handle_get_location_history)
        self.web_app.router.add_post("/api/get_closest_entry_by_timestamp", self.backend.handle_get_closest_entry_by_timestamp)
        self.web_app.router.add_route("*", "/{tail:.*}", self.backend.handle_404)  # catch-all for 404
        logging.info("Web application instance created")

    def create_ssl_context(self):
        self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.ssl_context.load_cert_chain(certfile=self.config.get("webserver", "server_cert"), keyfile=self.config.get("webserver", "server_key"))
        logging.info("SSL context created")

    def server_loop(self):
        logging.info("Starting server loop")
        web.run_app(self.web_app, port=self.config.getint("webserver", "port"), ssl_context=self.ssl_context)

if __name__ == "__main__":
    main = Main()
    main.server_loop()