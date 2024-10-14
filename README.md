# Kielipankki-Metax-bridge

This tool enables fetching metadata records from META-SHARE via its OAI-PMH API. It transforms and maps XML data to Metax accepted JSON format and pushes the data to Metax.

Furthermore, the tool checks whether records' PIDs in Kielipankki OAI-PMH API match those in Metax. If there are extra PIDs in Metax, their records are deleted from Metax.

## Running the program


### Requirements
This tool works with Python 3.8 together with pip and virtualenv installed. The rest of the requirements can be installed with ```pip install -r requirements.txt```.


### Run the whole pipeline

You can run the full havest process (fetch new records from Comedi, send them to Metax, and delete deleted records from Metax). This requires that you specify the path to the log file for logging timestamps for previous harvests and a configuration file that holds the API token for the Metax API. See `config/template.yml`.

```
python metadata_harvester_cli.py config/config.yml
```


## Development
To install additional requirements needed when development work, install requirements from requirements_dev.txt:
```pip install -r requirements_dev.txt```

### Running unit tests
```pytest```

Add -v to show individual unit test results:
```pytest -v```
