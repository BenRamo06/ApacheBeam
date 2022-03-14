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
        
        parser.add_value_provider_argument('--table_name', type =str)

        parser.add_value_provider_argument('--input_files', type =str)

        parser.add_value_provider_argument('--output_files', type =str)

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


class ImportBQ(beam.PTransform):
    def __init__(self, name_table):
        self.name_table = name_table

    def expand(self, collection):

        return (collection | 'Go to BQ' >> beam.io.WriteToBigQuery(table=self.name_table,
                                                                  schema='ID:NUMERIC,INIDATE:DATE,AMOUNT:NUMERIC', 
                                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                                  additional_bq_parameters={'timePartitioning':{'type':'DAY','field':'INIDATE'}}))

    

class ExportGCS(beam.PTransform):
    def __init__(self, name_path, name_table, name_export):
        self.name_path = name_path
        self.name_table = name_table
        self.name_export = name_export

    def expand(self, collection):

        return (
                collection | 'Export ' + self.name_export >> beam.io.WriteToText(self.name_path)
               )
    


with beam.Pipeline(options = PipelineOptions(options = process_option, save_main_session=True)) as pipeline:

    read_file = pipeline | 'Read' >> beam.io.ReadFromText(file_pattern = process_option.input_files, skip_header_lines = 1)

    valids, invalids, errors = read_file | 'Amount grater or equal than 0' >> beam.ParDo(divide_data('%d-%m-%y')).with_outputs('valids', 'invalids', 'errors')

    exportBQ = valids | 'Export BQ' >> ImportBQ(name_table=process_option.table_name)

    export_invalids = invalids | 'Export Invalids' >> ExportGCS(name_path=process_option.output_files, name_table=process_option.table_name, name_export= 'invalids')

    

# https://partly-cloudy.co.uk/2020/11/07/building-customizable-and-reusable-pipeline-using-dataflow-template/ 

# python3 -m test \
#  --region us-central1 \
#  --runner DataflowRunner \
#  --project cosmic-bonfire-313519 \
#  --temp_location gs://misarchivos/temp/ \
#  --template_location gs://misarchivos/templates/TEMPLATE_TEST
#  ##--experiment=use_beam_bq_sink 


#  gcloud dataflow jobs run test_execution \
#  --gcs-location gs://misarchivos/templates/TEMPLATE_TEST \
#  --region us-central1 \
#  --parameters table_name=cosmic-bonfire-313519:misdatos.prueba2,input_files=gs://misarchivos/dataflow_input/cambio.txt,output_files=gs://misarchivos/dataflow_output/