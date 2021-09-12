import apache_beam as beam

# We create a PCollection using a list
# We use beam.Map to print the result

with beam.Pipeline() as pipeline:
    (pipeline | 'Create PCollection' >> beam.Create([[1,10.3,'EUA'],
                                                     [1,10.3,'MEX']])
              | 'Print'              >> beam.Map(print)
    )