import apache_beam as beam

# Create Pipeline without options
pipeline = beam.Pipeline()

# Create PCollection with a list
# We use function Map with print and We see the result
(pipeline | 'Create PCollection' >> beam.Create([(1,10.3,'EUA'),
                                                 (1,10.3,'MEX')])
          | 'Print'              >> beam.Map(print))

# Execute Pipeline
pipeline.run().wait_until_finish()