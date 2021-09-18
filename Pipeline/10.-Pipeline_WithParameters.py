import argparse
import apache_beam as beam

from sys import argv
from apache_beam.options.pipeline_options import PipelineOptions


parser = argparse.ArgumentParser()

parser.add_argument('--input_files',
                    required = False,
                    default = '',
                    help = 'Path files to process')

parser.add_argument('--output_files',
                    required = False,
                    default = '',
                    help = 'Path files to process')

args_cmd, args_beam = parser.parse_known_args()


with beam.Pipeline(options = PipelineOptions(args_beam)) as pipeline:

    read_file = pipeline | 'Read' >> beam.io.ReadFromText(file_pattern = args_cmd.input_files, skip_header_lines = 1)

    filter = read_file | 'Amount grater or equal than 0' >> beam.Filter(lambda x : float(x.split(',')[3]) > 0)

    export = filter | 'Export data' >> beam.io.WriteToText(file_path_prefix = str(args_cmd.output_files).split('.')[0],
                                                           file_name_suffix = '.' + str(args_cmd.output_files).split('.')[1])


# Execution

# python3 Pipeline/10.-Pipeline_WithParameters.py \
# --input_files "inputs/InfoDataflow" \
# --output_files "outputs/Output.txt" \
# --runner DirectRunner
