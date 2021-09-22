import apache_beam as beam
import argparse
import time
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions


class cast_to_date(beam.DoFn):
    def __init__(self, formats):
        self.formats = formats

    def process(self,element):
        for i in self.formats:
            try:
                element[i[0]] = (datetime.strptime(element[i[0]],i[1]).strftime('%Y-%m-%d'))
            except:
                element[i[0]] = ''
        yield element


def to_json(element):
    return  {
                'id': str(element[0]),
                "amount" : element[1],
                "process_time" :  int(time.mktime(datetime.utcnow().timetuple()))
            }

with beam.Pipeline() as pipe:

    read_file = (pipe   | 'Read File' >> beam.io.ReadFromText('inputs/InfoDataflow', skip_header_lines = 1)
                        | 'Split' >> beam.Map(lambda x: x.split(','))
                )


    conversions = read_file | 'Cast Dates' >> beam.ParDo(cast_to_date(formats = [[1,'%d-%m-%y']]))


    group = (conversions | 'Group' >> beam.Map(lambda x: (x[0],float(x[2])))
                         | 'Sum' >> beam.CombinePerKey(sum)
            )

    to_json_data = group | 'Convert to json' >> beam.Map(to_json)

    print_data = to_json_data | beam.Map(print)