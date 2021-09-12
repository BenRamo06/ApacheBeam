import apache_beam as beam


# Read File
with beam.Pipeline() as pipeline:
    
    read_file = (pipeline | 'Read File' >> beam.io.ReadFromText('/content/sample_data/InfoDataflow', skip_header_lines = 1)
                          | beam.Map(print))
