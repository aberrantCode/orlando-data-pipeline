import datetime
import os
from google.cloud import storage
import requests
from xml.etree import ElementTree

#ensure the GCP credentials are set locally
credFile = os.getcwd() + "\\credentials.json"
if os.path.isfile(credFile):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credFile

#load existing calls for today
todayStr = datetime.date.today().strftime("%Y%m%d")
fileName = todayStr + ".csv"
client = storage.Client()
bucket = client.get_bucket('police-active-calls')
blob = bucket.get_blob(fileName)
if blob is not None:
    blobContent = blob.download_as_string().decode("utf8","text")
    print("Data File Downloaded: " + fileName)
else:
    print("Data File Missing: " + fileName)
    blob = bucket.blob(fileName)
    blobContent = ""

#fetch and save calls
response = requests.get("http://www1.cityoforlando.net/opd/activecalls/")
calls = ElementTree.fromstring(response.content)
for call in calls:
    incidentId = call.attrib["incident"]
    if incidentId in blobContent:
        print("Incident Already Stored")
    else:
        print("New Incident: " + incidentId)
        row = [incidentId, call[0].text, call[1].text, call[2].text, call[3].text]
        rowText = ','.join('"%s"'%i for i in row)
        blobContent = blobContent + rowText + "\r\n"

#store today's calls
blob.upload_from_string(blobContent, content_type='text/plain')

print("done")