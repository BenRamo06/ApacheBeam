import json
import logging
import argparse
import apache_beam as beam

from sys import argv
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions

logging.getLogger().setLevel(logging.INFO)

parser = argparse.ArgumentParser()

parser.add_argument('--table_name',
                    required = False,
                    default = '',
                    help = 'Name table')

parser.add_argument('--input_subscription',
                    required = False,
                    default = '',
                    help = 'Path files to process')

args_cmd, args_beam = parser.parse_known_args()

class getJSON(beam.DoFn):

    def process(self, element):
        import json

        row = element.decode("utf-8")
        
        record = json.loads(row)

        yield record



with beam.Pipeline(options = PipelineOptions(args_beam)) as pipeline:

    read_file = pipeline | 'Read' >> beam.io.ReadFromPubSub(subscription=args_cmd.input_subscription).with_output_types(bytes)

    cast_json = read_file | "Parse JSON to Dict" >> beam.ParDo(getJSON() )

    exportBQ = cast_json | 'Export BQ' >> beam.io.WriteToBigQuery(table='prueba', dataset='misdatos', project='cosmic-bonfire-313519', 
                                                               schema='ID:NUMERIC,INIDATE:DATE,AMOUNT:NUMERIC', 
                                                               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                               write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)



# python3 -m test2 \
# --table_name prueba \
# --input_subscription projects/cosmic-bonfire-313519/subscriptions/test_data-sub \
# --region us-central1 \
# --runner DataflowRunner \
# --project cosmic-bonfire-313519 \
# --temp_location gs://misarchivos/temp/ \
# --save_main_session True \
# --streaming True