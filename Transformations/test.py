import apache_beam as beam
import argparse
import time
import json
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.core import Map



class cast_to_datetime(beam.DoFn):
    def __init__(self, formats):
        self.formats = formats

    def process(self,element):
        for i in self.formats:
            try:
                element[i[0]] = int(time.mktime((datetime.strptime(element[i[0]], i[1]).timetuple())))
            except:
                element[i[0]] = ''
        yield element


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


def add_date(element, ):
    row = element
    row['process_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    return row



class valid_strcuture(beam.DoFn):
    def __init__(self, structure):
        self.structure = structure

    def process(self, element):
        
        if len(self.structure.split(',')) == len(element):
            yield beam.pvalue.TaggedOutput('valids', dict(zip(self.structure.split(','), element)))
        else:
            yield beam.pvalue.TaggedOutput('fails', ','.join(element))



def to_json(element):
    return  json.dumps( {
                            "id": str(element[0]),
                            "amount" : element[1],
                            "process_time" :  int(time.mktime(datetime.utcnow().timetuple()))
                        })

with beam.Pipeline() as pipe:

    read_file    = (pipe     | 'Read File' >> beam.io.ReadFromText('inputs/InfoDataflow', skip_header_lines = 1)
                             | 'Split' >> beam.Map(lambda x: x.split(',')))

    valid, fails = read_file | 'Valid Rows' >> beam.ParDo(valid_strcuture(structure = 'id,date,amount1,amount2,amount3,amount4,refer')).with_outputs('valids', 'fails')

    amount       = valid     | 'Filter > 0' >> beam.Map(lambda x: float(x['amount1']) > 0)
    
    conversions  = (valid    | 'Cast Dates' >> beam.ParDo(cast_to_date(formats = [['date','%m-%d-%y']]))
                             | 'Add Date'   >> beam.Map(add_date))
                             #| 'Parse Json' >> beam.Map(lambda x: json.dumps(x))
                   

    group        = conversions | 'Group' >> beam.Map(lambda x: ((x['id'],x['process_date']),float(x['amount1'])))


    sum_group    = group | 'Sum by Key' >> beam.CombinePerKey(sum)

    print_data = sum_group | beam.Map(print)