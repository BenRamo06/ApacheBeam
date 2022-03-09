import logging
import argparse
import apache_beam as beam

from sys import argv
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import StaticValueProvider

logging.getLogger().setLevel(logging.INFO)




class TemplateOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):


        # We change add_argument by add_value_provider_argument
        parser.add_value_provider_argument('--table_name',
                                            required = False,
                                            default = '',
                                            help = 'Name table')


        parser.add_value_provider_argument('--input_files',
                            required = False,
                            default = '',
                            help = 'Path files to process')

        parser.add_value_provider_argument('--output_files',
                            required = False,
                            default = '',
                            help = 'Path files to process')


# We create PipeliOptions as view_as, we call class with add_value_provider_argument(s)
process_option = PipelineOptions().view_as(TemplateOptions)

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

with beam.Pipeline(options = PipelineOptions(options = process_option)) as pipeline:

    read_file = pipeline | 'Read' >> beam.io.ReadFromText(file_pattern = process_option.input_files, skip_header_lines = 1)

    valids, invalids, errors = read_file | 'Amount grater or equal than 0' >> beam.ParDo(divide_data('%d-%m-%y')).with_outputs('valids', 'invalids', 'errors')

    exportBQ = valids | 'Export BQ' >> beam.io.WriteToBigQuery(table='prueba', dataset='misdatos', project='cosmic-bonfire-313519', 
                                                               schema='ID:NUMERIC,INIDATE:DATE,AMOUNT:NUMERIC', 
                                                               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                               write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                               additional_bq_parameters={'timePartitioning':{'type':'DAY','field':'INIDATE'}})


    exportGCS = invalids | 'Export GCS' >> beam.io.WriteToText(file_path_prefix='{0}{1}/invalids/'.format(process_option.output_files, process_option.table_name), file_name_suffix= '.txt')

    exportErrors = errors | 'Export Errors' >> beam.io.WriteToText(file_path_prefix='{0}{1}/errors/'.format(process_option.output_files, process_option.table_name), file_name_suffix= '.txt')






# -- Create Template (template_location) --
# python3 -m test \
#  --table_name prueba \
#  --input_files gs://misarchivos/dataflow_input/salida.txt \
#  --output_files gs://misarchivos/dataflow_output/ \
#  --region us-central1 \
#  --runner DataflowRunner \
#  --project cosmic-bonfire-313519 \
#  --temp_location gs://misarchivos/temp/ \
#  --template_location gs://misarchivos/templates/TEMPLATE_TEST



# --- Use Template ---
# gcloud dataflow jobs run test_execution \
#  --gcs-location gs://misarchivos/templates/TEMPLATE_TEST \
#  --parameters table_name=prueba,input_files=gs://misarchivos/dataflow_input/salida.txt,output_files=gs://misarchivos/dataflow_output/