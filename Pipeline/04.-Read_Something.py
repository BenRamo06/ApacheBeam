import apache_beam as beam

# We created a pipeline without arguments
with beam.Pipeline() as pipeline:
    
    # We created branch "read_file" to Read file from text
    read_file = (pipeline | 'Read File' >> beam.io.ReadFromText('inputs/InfoDataflow', skip_header_lines = 1)
                          | beam.Map(print))
