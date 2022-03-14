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



## Install virtualenv package: 
#pip3 install virtualenv
## Create new environment:
#python3 -m virtualenv env
## Activate environment:
#source env/bin/activate

## Install libraries to GCP
# pip3 install apache-beam[gcp]
# pip3 install google-cloud-storage

## Exit environment
# deactivate
## Remove environment
# rm -rf env




# rm Python-3.8.0.tgz
# rm -rf Python-3.8.0
#-- Download version Python
#wget https://www.python.org/ftp/python/3.8.0/Python-3.8.0.tgz
#-- unzip file
#tar -xf Python-3.8.0.tgz
#-- go to file
#cd Python-3.8.0
#-- install version python
#./configure --enable-optimizations
#make -j 8
#sudo make altinstall
#-- Check version python
#python3.8 --version

# virtualenv --python=/usr/local/bin/python3.8 <enviroment>
# source <enviroment>/bin/activate

# pip3 install apache-beam[gcp]
# pip3 install google-cloud-storage