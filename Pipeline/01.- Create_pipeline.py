# Import libraries of Apache Beam
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# We can create a pipeline two forms:

# 1) We can use with form for create a Pipeline, and it will execute automatically

with beam.Pipeline() as pipeline:
    pass


# 2) We can use variable form for create a Pipeline 
#    but we need to execute the pipeline manually with sentecen run

pipeline = beam.Pipeline()

pipeline.run().wait_until_finish()