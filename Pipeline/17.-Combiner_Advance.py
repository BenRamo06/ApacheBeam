import apache_beam as beam

class Average(beam.CombineFn):

    def create_accumulator(self):
        return (0.0, 0) # Initialize acummulator 

    def add_input(self, accumulator, element):
        (sum,count) = accumulator
        return sum + element, count + 1
    
    def merge_accumulators(self, accumulators):
        ind_sums, ind_counts = zip(*accumulators) # zip - [(23,3),(39,3),(18,2)]  ---> [(27,39,18),(3,3,2)]
        return sum(ind_sums), sum(ind_counts)     # (84,8)
 
    def extract_output(self, accumulator):
        (sum, count) = accumulator
        return sum / count if count else float('NaN')   # 10.5
        


with beam.Pipeline() as pipe:
    
    data = pipe | 'Read Data' >> beam.Create([15,5,7,7,9,23,13,5])

    acum = data | 'Get Acum' >> beam.CombineGlobally(Average())

    print_data = acum | 'Print' >> beam.Map(print)

