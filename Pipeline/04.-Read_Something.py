import apache_beam as beam

# Read File
with beam.Pipeline() as pipeline:
    
    # Read file from text
    read_file = (pipeline | 'Read File' >> beam.io.ReadFromText('inputs/InfoDataflow', skip_header_lines = 1)
                          | beam.Map(print))
