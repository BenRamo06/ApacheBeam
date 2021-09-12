import apache_beam as beam



with beam.Pipeline() as pipeline:
    # Create branch "read_file" to read file
    read_file = (pipeline | 'Read File' >> beam.io.ReadFromText('inputs/InfoDataflow', skip_header_lines = 1))

    # Create branch "output_file" with "read_file" as source to export data
    output_file = (read_file | 'Export' >> beam.io.WriteToText('outputs/InfoDataflow_output'))
