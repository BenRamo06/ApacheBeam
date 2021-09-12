import apache_beam as beam

# We created a pipeline without arguments
with beam.Pipeline() as pipeline:

# Create PCollection with a list
# We use function Map with print and We see the result
    (pipeline | 'Create PCollection' >> beam.Create([(1,10.3,'EUA'),
                                                     (1,10.3,'MEX')])
              | 'Print'              >> beam.Map(print))

