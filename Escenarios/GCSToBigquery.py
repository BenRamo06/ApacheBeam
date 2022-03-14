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


parser.add_argument('--input_files',
                    required = False,
                    default = '',
                    help = 'Path files to process')

parser.add_argument('--output_files',
                    required = False,
                    default = '',
                    help = 'Path files to process')


args_cmd, args_beam = parser.parse_known_args()


class divide_data(beam.DoFn):

    def __init__(self, formatDate):
        self.formatDate = formatDate

    def process(self, element):
        import apache_beam as beam
        from datetime import datetime

        row = element.split(',')

        try:
            if float(row[3]) > 0:
                row[1] = datetime.strptime(row[1], self.formatDate).strftime('%Y-%m-%d')
                logging.info('element valid:', row)
                yield beam.pvalue.TaggedOutput('valids', dict(zip(['ID','INIDATE','AMOUNT'],[int(row[0]), row[1], float(row[3])])))
            else:
                yield beam.pvalue.TaggedOutput('invalids', element)
        except:
            yield beam.pvalue.TaggedOutput('errors', element)

with beam.Pipeline(options = PipelineOptions(args_beam)) as pipeline:

    read_file = pipeline | 'Read' >> beam.io.ReadFromText(file_pattern = args_cmd.input_files, skip_header_lines = 1)

    valids, invalids, errors = read_file | 'Amount grater or equal than 0' >> beam.ParDo(divide_data('%d-%m-%y')).with_outputs('valids', 'invalids', 'errors')

    exportBQ = valids | 'Export BQ' >> beam.io.WriteToBigQuery(table='prueba', dataset='misdatos', project='cosmic-bonfire-313519', 
                                                               schema='ID:NUMERIC,INIDATE:DATE,AMOUNT:NUMERIC', 
                                                               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                               write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                               additional_bq_parameters={'timePartitioning':{'type':'DAY','field':'INIDATE'}})


    exportGCS = invalids | 'Export GCS' >> beam.io.WriteToText(file_path_prefix='{0}{1}/invalids/'.format(args_cmd.output_files, args_cmd.table_name), file_name_suffix= '.txt')

    exportErrors = errors | 'Export Errors' >> beam.io.WriteToText(file_path_prefix='{0}{1}/errors/'.format(args_cmd.output_files, args_cmd.table_name), file_name_suffix= '.txt')


# python3 -m toBigquery \
#  --table_name prueba \
#  --input_files gs://misarchivos/dataflow_input/salida.txt \
#  --output_files gs://misarchivos/dataflow_output/ \
#  --region us-central1 \
#  --runner DataflowRunner \
#  --project cosmic-bonfire-313519 \
#  --temp_location gs://misarchivos/temp/