import graphviz
import apache_beam as beam
from datetime import datetime
from apache_beam.runners.interactive.display import pipeline_graph


class extract_data(beam.DoFn):
    def __init__(self):
        pass

    def process(self, element):

        if element[0] != '':
            yield element[0] 
        


with beam.Pipeline() as pipeline:
    
    read_file =  pipeline | "Read file" >> beam.io.ReadFromText('inputs/historic.txt')

    divide = read_file | "Divide file" >> beam.Map(lambda x: x.split("\t"))
                    
    groups = (divide | "Create groups" >> beam.ParDo(extract_data())
    #                  | "Sum Group By"  >> beam.CombinePerKey(sum)
             ) 


    print_data = groups | beam.Map(print)


    # def display_pipeline(pipeline):
    #     graph = pipeline_graph.PipelineGraph(pipeline)
    #     return graphviz.Source(graph.get_dot())

    # display_pipeline(pipeline)