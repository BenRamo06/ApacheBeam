import apache_beam as beam
import argparse
import logging
import json
import time
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
from apache_beam import DoFn

def definition():
    params = argparse.ArgumentParser()

    params.add_argument('--input_files',
                        required = False,
                        default = '',
                        help = 'Path input file',
                        type = str)


    params.add_argument('--output_files',
                        required = False,
                        default = '',
                        help = 'Path output file',
                        type = str)

    
    args_cmd, args_beam = params.parse_known_args()

    return args_cmd, args_beam
    

class AddDateTimestamp(beam.DoFn):

    def process (self, element):

        yield {'id': element['id'],
                'salary' : element['process']}

        




def etl(args_cmd, args_beam):
    
    with beam.Pipeline(options = PipelineOptions(args_beam) ) as pipe:

        read_file  = pipe | 'Read Files' >> beam.io.ReadFromText(args_cmd.input_files)

        cast_json  = read_file | 'Parse JSON' >> beam.Map(json.loads)

        extract_json = cast_json | 'Extract JSON' >> beam.ParDo(AddDateTimestamp())
    
        print_data = extract_json | 'Print' >> beam.Map(print) 

if __name__ == "__main__":
    # logging.getLogger().setLevel(logging.INFO)

    args_cmd, args_beam = definition()

    # logging.info("Arguments CMD ------> {args_cmd}")
    # logging.info("--------------------> ")
    # logging.info("Arguments Beam -----> {args_beam}")

    etl(args_cmd, args_beam)