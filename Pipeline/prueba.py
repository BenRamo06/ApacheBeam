import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

with beam.Pipeline(options = PipelineOptions()) as pipeline:

    read_file = pipeline | 'Read' >> beam.io.ReadFromText('inputs/InfoDataflow', skip_header_lines = 1)

    filter = read_file | 'Amount grater or equal than 0' >> beam.Filter(lambda x : float(x.split(',')[3]) > 0)

    export = filter | 'Export data' >> beam.io.WriteToText(file_path_prefix = 'salida_',
                                                           file_name_suffix = '.txt')

