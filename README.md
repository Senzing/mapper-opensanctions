# mapper-opensanctions

This is a simple mapper to convert the OpenSanctions Consolidated Sanctioned Enttities simplified CSV list to JSON that can be loaded into Senzing.

https://www.opensanctions.org/datasets/sanctions/


To run the tool:
# install the orjson module
python3 -m pip install orjson

#convert the CSV to Senzing JSON
python3 opensanctions_process.py targets.simple.csv > os.json

Then add the OPENSANCTIONS data source to the Senzing configuration and load the os.json file.

