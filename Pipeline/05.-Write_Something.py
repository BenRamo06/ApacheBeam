import apache_beam as beam

# We created a pipeline without arguments
with beam.Pipeline() as pipeline:
    # We created branch named "read_file"
    read_file = (pipeline | 'Read File' >> beam.io.ReadFromText('inputs/InfoDataflow', skip_header_lines = 1))

    # We created branch "output_file" and use "read_file" as source to export data
    output_file = (read_file | 'Export' >> beam.io.WriteToText('outputs/InfoDataflow_output'))
