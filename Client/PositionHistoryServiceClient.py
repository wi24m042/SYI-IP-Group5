from zeep import Client
from zeep.transports import Transport
import requests

wsdl = "https://ntgddns.asuscomm.com:8000/?wsdl"

session = requests.Session()

client = Client(wsdl=wsdl, transport=Transport(session=session))

print("Location History:")
result = client.service.GetLocationHistory(
    1748530800,
    1748531400
)

print(result)
print(type(result))

print("Closest Entry by Timestamp:")
result = client.service.GetClosestEntryByTimestamp(
    1748638740
)

print(result)
print(type(result))