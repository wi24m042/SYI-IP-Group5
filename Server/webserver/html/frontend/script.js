var map, marker;
var selectedTimestamp = null;
var selectedStartTime = null;
var selectedStopTime = null;
var DatePickerTimestamp = null;
var DatePickerStartTime = null;
var DatePickerStopTime = null;
var issMarkers = [];
console.log
const RestHeaders = new Headers();
RestHeaders.append('Content-Type', 'application/json');
const SOAPHeaders = new Headers();
SOAPHeaders.append('Content-Type', 'application/soap+xml');

const HomeIcon = L.IconMaterial.icon(
{
  icon: 'home',
  iconColor: 'black',
  markerColor: '#00649C',
  outlineColor: '#2c2f3a',
  outlineWidth: 1,
  iconSize: [31, 42]
});

const SatelliteIcon = L.IconMaterial.icon(
{
  icon: 'satellite_alt',
  iconColor: 'black',
  markerColor: '#8BB31D',
  outlineColor: '#2c2f3a',
  outlineWidth: 1,
  iconSize: [31, 42]
});

function createMap()
{
  map = L.map('map', {
    zoomControl: false,
    worldCopyJump: false, 
    maxBounds: [[-85, -180], [85, 180]], 
    maxBoundsViscosity: 1.0 
  }).setView([0, 0], 2);

  L.control.zoom({ position: 'bottomright' }).addTo(map);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', 
    {
      maxZoom: 19,
      attribution: '© OpenStreetMap | Position History Service for ISS by SYI Integration Project Group 5',
    }
  ).addTo(map);
}

function addUserLocationMarker(position)
{
  var lat = position.coords.latitude;
  var lon = position.coords.longitude;

  marker = L.marker([lat, lon], {icon: HomeIcon}).addTo(map).bindPopup("You are here!<br>Latitude: " + lat + "<br>Longitude: " + lon).bindTooltip("You are here!<br>Latitude: " + lat + "<br>Longitude: " + lon);
  map.setView([lat, lon], 2);
}

function handleLocationError(error) 
{
  console.log("Error getting location: " + error.message);
}

function getLocation() 
{
  if (navigator.geolocation)
  {
    navigator.geolocation.getCurrentPosition(addUserLocationMarker, handleLocationError);
  } 
  else 
  {
    alert("Geolocation is not supported by this browser.");
  }
}

async function perform_rest_request(RestRequest)
{
  console.log("Performing REST request:", RestRequest);
  const RestResponse = await fetch(RestRequest);
  const JSON_List = await RestResponse.json();
  console.log("REST response JSON:", JSON_List);
  JSON_List.forEach(entry => 
  {
    const timestamp = entry.timestamp;
    const latitude = entry.latitude;
    const longitude = entry.longitude;
    const source = entry.source;
    const timestamp_human_readable = new Date(timestamp * 1000).toLocaleString();
    issMarker = L.marker([latitude, longitude], {icon: SatelliteIcon}).addTo(map).bindPopup("Latitude: " + latitude + "<br>Longitude: " + longitude + "<br>Timestamp: " + timestamp_human_readable + "<br>Source: " + source).bindTooltip("Latitude: " + latitude + "<br>Longitude: " + longitude + "<br>Timestamp: " + timestamp_human_readable + "<br>Source: " + source);
    issMarkers.push(issMarker);
  });  
}

async function perform_soap_request(SoapBody, ResponseNamespace) 
{
  console.log("Performing SOAP request with body:", SoapBody);
  const SoapResponse = await fetch('https://ntgddns.asuscomm.com:8000/', 
  {
    method: 'POST',
    headers: SOAPHeaders,
    body: SoapBody
  });

  const SoapResponseContent = await SoapResponse.text();
  const XmlParser = new DOMParser();
  const XmlDoc = XmlParser.parseFromString(SoapResponseContent, "application/xml");
  const XmlEntries = XmlDoc.getElementsByTagNameNS("*", ResponseNamespace);
  console.log("SOAP response XML:", XmlEntries);
  Array.from(XmlEntries).forEach(entry => 
  {
    const timestamp = parseInt(entry.getElementsByTagNameNS("*", "timestamp")[0].textContent);
    const latitude = parseFloat(entry.getElementsByTagNameNS("*", "latitude")[0].textContent);
    const longitude = parseFloat(entry.getElementsByTagNameNS("*", "longitude")[0].textContent);
    const source = entry.getElementsByTagNameNS("*", "source")[0].textContent;

    const timestamp_human_readable = new Date(timestamp * 1000).toLocaleString();
    issMarker = L.marker([latitude, longitude], {icon: SatelliteIcon}).addTo(map).bindPopup("Latitude: " + latitude + "<br>Longitude: " + longitude + "<br>Timestamp: " + timestamp_human_readable + "<br>Source: " + source).bindTooltip("Latitude: " + latitude + "<br>Longitude: " + longitude + "<br>Timestamp: " + timestamp_human_readable + "<br>Source: " + source);
    issMarkers.push(issMarker);
  });
}

function get_location_history()
{  
  console.log("Getting location history from " + selectedStartTime + " to " + selectedStopTime);
  const selectedOption = document.querySelector('input[name="choice"]:checked');
  if (selectedOption.value == "rest")
  {
    const RestRequest = new Request(
      location.protocol + "//" + location.hostname + ":" + location.port + "/api/get_location_history", 
      {
        method: 'POST',
        body: JSON.stringify({"PositionHistoryService": {"GetLocationHistory": {"StartTime": selectedStartTime, "StopTime": selectedStopTime}}}),
        headers: RestHeaders,
      }
    );
    perform_rest_request(RestRequest);
  }
  else if (selectedOption.value == "soap")
  {
    const SoapBody = `
      <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                        xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
        <soap12:Body>
          <GetLocationHistory xmlns="http://technikum-wien.at/mwi/integration-project/group5/position-history-service">
            <start_time>${selectedStartTime}</start_time>
            <stop_time>${selectedStopTime}</stop_time>
          </GetLocationHistory>
        </soap12:Body>
      </soap12:Envelope>
    `;
    perform_soap_request(SoapBody, "LocationRecord");
  }
}

function get_closest_entry_by_timestamp()
{
  console.log("Getting closest entry by timestamp:", selectedTimestamp);
  const selectedOption = document.querySelector('input[name="choice"]:checked');
  if (selectedOption.value == "rest")
  {
    const RestRequest = new Request(
      location.protocol + "//" + location.hostname + ":" + location.port + "/api/get_closest_entry_by_timestamp",
      {
        method: 'POST',
        body: JSON.stringify({"PositionHistoryService": {"GetClosestEntryByTimestamp": {"Timestamp": selectedTimestamp}}}),
        headers: RestHeaders,
      }
    );
    perform_rest_request(RestRequest);
  }
  else if (selectedOption.value == "soap")
  {
    const SoapBody = `
      <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                        xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
        <soap12:Body>
          <GetClosestEntryByTimestamp xmlns="http://technikum-wien.at/mwi/integration-project/group5/position-history-service">
            <timestamp>${selectedTimestamp}</timestamp>
          </GetClosestEntryByTimestamp>
        </soap12:Body>
      </soap12:Envelope>
    `;
    perform_soap_request(SoapBody, "GetClosestEntryByTimestampResult");
  }
}

function removeIssMarkers() 
{
  issMarkers.forEach(marker => map.removeLayer(marker));
  issMarkers = [];
}

function configureDateTimePicker(inputId, input, setSelectedValue) 
{
  var thisFlatpickr = flatpickr(input, 
  {
    locale: {
        firstDayOfWeek: 1
    },
    enableTime: true,
    dateFormat: "d.m.Y H:i",
    time_24hr: true,
    minuteIncrement: 1,
    minDate: new Date(2025,5,3, 2,0,0),
    onChange: function(selectedDates) 
    {
      if (selectedDates.length > 0) 
      {
        setSelectedValue(Math.floor(selectedDates[0].getTime() / 1000));
      } 
      else 
      {
        setSelectedValue(null);
      }
      if (inputId == 'get_location_history_form_input_starttime' && DatePickerStopTime) 
      {
        DatePickerStopTime.set('minDate', selectedDates[0] || null);
      }
      if (inputId == 'get_location_history_form_input_stoptime' && DatePickerStartTime) 
      {
        DatePickerStartTime.set('maxDate', selectedDates[0] || null);
      }
    },
    onOpen: function(selectedDates, dateStr, instance)
    {
      if (inputId == 'get_closest_entry_by_timestamp_form_input_timestamp') 
      {
        currentTimestamp = new Date();
        DatePickerTimestamp.set('maxDate', currentTimestamp);
        if (dateStr === "") {
          instance.setDate(currentTimestamp);
          setSelectedValue(Math.floor(currentTimestamp.getTime() / 1000));
        }
      }
      if (inputId == 'get_location_history_form_input_starttime') 
      {
        currentTimestamp = new Date();
        DatePickerStartTime.set('maxDate', currentTimestamp);
        if (dateStr === "") {
          instance.setDate(new Date(2025,5,3, 2,0,0));
          setSelectedValue(Math.floor(currentTimestamp.getTime() / 1000));
        }
      }
      if (inputId == 'get_location_history_form_input_stoptime') 
      {
        currentTimestamp = new Date();
        DatePickerStopTime.set('maxDate', currentTimestamp);
        if (dateStr === "") {
          instance.setDate(currentTimestamp);
          setSelectedValue(Math.floor(currentTimestamp.getTime() / 1000));
        }
      }
    }
  });
  return thisFlatpickr;
}

function configureDateTimePickerInstance(inputId, setSelectedValue, dateTimePicker) 
{
  var setSelectedValueLocal = null;
  const input = document.getElementById(inputId);
  const dateTimePickerLocal = configureDateTimePicker(inputId, input, val => setSelectedValueLocal = setSelectedValue(val));
  input.addEventListener('focus', function() 
  {
    dateTimePickerLocal;
  });
  dateTimePicker(dateTimePickerLocal);
}

function configureSubmitButton(buttonId, isValid, onSubmit) 
{
  document.getElementById(buttonId).addEventListener('click', function() 
  {
    if (isValid()) 
    {
      removeIssMarkers();
      onSubmit();
    } 
    else 
    {
      console.log("No date selected.");
    }
  });
}


document.addEventListener("DOMContentLoaded", () => {
  const sidebar = document.getElementById("sidebar");
  const toggleBtn = document.getElementById("sidebarToggle");
  const toggleIcon = document.getElementById("toggleIcon");

  toggleBtn.addEventListener("click", () => {
    sidebar.classList.toggle("hidden");

    const isHidden = sidebar.classList.contains("hidden");
    toggleIcon.textContent = isHidden ? "▶" : "◀";

    setTimeout(() => {
      if (typeof map !== "undefined") {
        map.invalidateSize();
      }
    }, 310);
  });
});



window.onload = function() 
{
  createMap();
  getLocation();
  configureDateTimePickerInstance('get_closest_entry_by_timestamp_form_input_timestamp', val => selectedTimestamp = val, val => DatePickerTimestamp = val);
  configureDateTimePickerInstance('get_location_history_form_input_starttime', val => selectedStartTime = val, val => DatePickerStartTime = val);
  configureDateTimePickerInstance('get_location_history_form_input_stoptime', val => selectedStopTime = val, val => DatePickerStopTime = val);
  configureSubmitButton(
    'get_closest_entry_by_timestamp_form_button_submit',
    () => !!selectedTimestamp,
    () => get_closest_entry_by_timestamp()
  );
  configureSubmitButton(
    'get_location_history_form_button_submit',
    () => !!selectedStartTime && !!selectedStopTime,
    () => get_location_history()
  );
};