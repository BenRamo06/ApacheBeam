import json
import logging
import apache_beam as beam
from datetime import datetime

def filter_data(element):
    
    try:
        year = int(datetime.strptime(element['dob'], '%Y-%m-%d').strftime('%Y'))
        # These log lines will appear in the Cloud Logging UI.
        logging.info('element valid:', element)
    except:
        # Currently only "INFO" and higher level logs are emitted to the Cloud Logger
        # This log message will not be visible in the Cloud Logger.
        logging.debug('Error element:', element)

    if year >= 1980:
        return  element

    

with beam.Pipeline() as pipe:

    read_file    = (pipe    | 'Read File' >> beam.io.ReadFromText('inputs/jsonFile')
                            | 'Load Json' >> beam.Map(lambda x: json.loads(x)))


    filter_data  = read_file | 'Filter data' >> beam.Map(filter_data)

    print_data = filter_data | 'Print' >> beam.Map(print)

