
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems

from data.customers import Customers
from utils.parsers import customers_parser
from extract.parser import parse_files


import apache_beam as beam
import argparse
import logging
import json

def run(argv= None):
    
    parser = argparse.ArgumentParser()

    parser.add_argument('--file', required= True, help="Path file to load")

    command_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=pipeline_args) as pipeline:
      
      
      customer_file = pipeline | "Read Customers" >> parse_files(path=command_args.file,
                                                                  delimiter=",",
                                                                  parser=customers_parser)
                                                                  

      print_data = customer_file | "Print" >> beam.Map(print)
       

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

