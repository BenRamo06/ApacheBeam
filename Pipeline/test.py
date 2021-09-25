import apache_beam as beam

def sum_l(l):                       
    s0, s1 = 0, 0                                         
    for i in range(len(l)):                                        
        s0 += float(l[i][0])                                                      
        s1 += float(l[i][1])                
    return [s0, s1] 

with beam.Pipeline() as p:
    read =  (p  | 'Read Input' >> beam.Create([ 'name1,1,place1,2.,1.5',
                                                'name1,1,place1,3.,0.5',
                                                'name1,1,place2,1.,1',
                                                'name1,2,place3,2.,1.5',
                                                'name2,2,place3,3.,0.5'
                                             ])
                | 'Split Commas' >> beam.Map(lambda x: x.split(','))
                | 'Prepare Keys' >> beam.Map(lambda x: (x[:-2], x[-2:]))
                | 'Group Each Key' >> beam.GroupByKey()
                | 'Make Summation' >> beam.Map(lambda x: [x[0], sum_l([e for e in x[1]])])
                #| 'Write Results' >> beam.io.WriteToText('results.csv'))
    )


    print_data = read | 'Print' >> beam.Map(print)