import os

# Run and print a shell command
def run(cmd):
  os.system(cmd)
  print('Command finished >> {}'.format(cmd))

# Update and upgrade the system before installing anything else.
run('apt-get update')
run('apt-get upgrade')

# Install apache-beam 
run('pip install apache-beam')

# Install libraries to GCP
#run('pip install apache-beam[gcp]')
#run('pip install apache-beam[interactive]')
#run('pip install google-apitools')