import apache_beam as beam

def Divide_Rows(row):
  return row.split(',')


with beam.Pipeline() as pipeline:
    
    read_file = (pipeline | 'Read File' >> beam.io.ReadFromText('inputs/InfoDataflow', skip_header_lines = 1)
                          # We create a transformation with Map and DoFn
                          # If we use Map, we need to create a function, class or lambda
                          | 'Divide row' >> beam.Map(Divide_Rows)
                          | beam.Map(print)
                )