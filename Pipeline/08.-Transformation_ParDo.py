import apache_beam as beam

def Divide_Rows(row):
  yield row.split(',')


with beam.Pipeline() as pipeline:
    
    read_file = (pipeline | 'Read File' >> beam.io.ReadFromText('inputs/InfoDataflow', skip_header_lines = 1)
                          # We create a transformation with ParDo and DoFn
                          # If we use ParDo, we need to create iterable output 
                          | 'Divide row' >> beam.ParDo(Divide_Rows)
                          | beam.Map(print)
                )