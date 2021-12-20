data = {"id":"1","first_name":"John","last_name":"Doe","dob":"1968-01-22","process":"2021-09-01 11:15:00","salary":"9823.11","addresses":[{"status":"current","address":"123 First Avenue","city":"Seattle","state":"WA","zip":"11111","numberOfYears":"1"},{"status":"previous","address":"456 Main Street","city":"Portland","state":"OR","zip":"22222","numberOfYears":"5"}]}



salida = [',' + i['city'] for i in data['addresses'] if i['status'] == 'current' or i['status'] == 'previous']


print(salida)