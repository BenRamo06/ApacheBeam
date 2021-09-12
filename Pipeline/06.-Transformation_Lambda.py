import apache_beam as beam


with beam.Pipeline() as pipeline:
    
    read_file = (pipeline | 'Read File' >> beam.io.ReadFromText('inputs/InfoDataflow', skip_header_lines = 1)
                          # We create a transformation with Map and lambda function
                          # It scenario is when We have a transformation "easy"
                          | 'Divide row' >> beam.Map(lambda x: x.split(','))
                          | beam.Map(print))