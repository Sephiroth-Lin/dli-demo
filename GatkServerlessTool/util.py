import re
import os
import tempfile
import uuid
import logging.config
import getopt
import sys
import json
import urllib2
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# log config
logging.config.fileConfig('logging.config')
log=logging.getLogger('log')

usage = 'gatk_workflow.py' \
        '-t Input file type, only support fastq, ubam and bam. \n' \
        '-i,--F1 Input file path for single data, or first read in paired fastq end data. \n' \
        '-o   Output file path \n' \
        '--F2 optional, Input fastq file (optionally gzipped) for the second read of paired end data. \n' \
        '--SM optional, Sample name to insert into the read group header \n' \
        '--PL optional, The platform type (e.g. illumina, solid) to insert into the read group header \n' \
        '--MR optional, max count of fastq record split into one file \n' \
        '--V  optional, A value describing how the quality values are encoded in the input FASTQ file \n' \
        '--RG optional, Read group name \n' \
        '--LB optional, The library name to place into the LB attribute in the read group header \n' \
        '--PU optional, The platform unit(often run_barcode.lane) to insert into the read group header \n' \
        '--CN optional, The sequencing center from which the data originated \n' \
        '--PI optional, Predicted median insert size, to insert into the read group header \n' \
        '--PG optional, Program group to insert into the read group header \n' \
        '--PM optional, Platform model to insert into the group header ' \
        '(free-form text providing further details of the platform/technology used) \n' \
        '--CO optional, Comment(s) to include in the merged output file header \n' \
        '--DS optional, Description inserted into the read group header \n' \
        '--DT optional, Date the run was produced, to insert into the read group header \n' \
        '--SO optional, The sort order for the output sam/bam file \n'

# functions to get input path and output path
def get_in_output(argv, ak, sk):
    inputfile1 = ''
    outputfile = ''
    type = ''
    picard_args = []

    try:
       opts, args = getopt.getopt(argv,"hi:o:t:",["F1=","F2=","SM=","PL=","MR=","V=","RG=","LB=","PU=","CN=",
                                                  "PI=","PG=","PM=","CO=","DS=","DT=","SO="])
    except getopt.GetoptError:
       print usage
       sys.exit(2)

    for opt, arg in opts:
       if opt == '-h':
          print usage
          sys.exit()
       elif opt in ("-i", "--F1"):
          inputfile1 = format_path(arg, ak, sk)
          picard_args.append('F1=' + inputfile1)
       elif opt == "--F2":
          picard_args.append('F2=' + format_path(arg, ak, sk))
       elif opt in ("-o", "--ofile"):
          outputfile = format_path(arg, ak, sk)
       elif opt in ("-t", "--itype"):
          type = arg
       else:
           # translate '--SM' to 'SM='
           picard_args.append(opt[2:] + '=' + arg)

    if inputfile1 == '' or type == '' or outputfile == '':
        print usage
        sys.exit()
    return inputfile1, outputfile, type, picard_args


def format_path(path, ak, sk):
    if path.startswith('s3a:'):
        path_without_prefix = path[6:]
        while path_without_prefix[0:1] == '/':
            path_without_prefix = path_without_prefix[1 : ]
        # if this path already contains aksk will return directly
        bucketname = path_without_prefix[0 : path_without_prefix.index('/')]
        if ':' in bucketname and '@' in bucketname:
            return path
        return 's3a://' + ak + ':' + sk + '@' + path_without_prefix
    else:
        return path


# Functions to get token
def get_token(url, user_name, password, project_id):
    log.info('Trying to get token for user: ' + user_name)
    payload = {
        "auth": {
            "identity": {
                "password": {
                    "user": {
                        "password": password,
                        "domain": {
                            "name": user_name
                        },
                        "name": user_name
                    }
                },
                "methods": [
                    "password"
                ]
            },
            "scope": {
                "project": {
                    "id": project_id
                }
            }
        }
    }

    headers = {
        'content-type': "application/json"
    }

    req = urllib2.Request(url, data=json.dumps(payload), headers=headers)
    response = urllib2.urlopen(req)
    token = response.info()['x-subject-token']

    return token


# Functions to read config.property
class Properties:

    def __init__(self, file_name):
        self.file_name = file_name
        self.properties = {}
        try:
            fopen = open(self.file_name, 'r')
            for line in fopen:
                line = line.strip()
                if line.find('=') > 0 and not line.startswith('#'):
                    strs = line.split('=')
                    self.properties[strs[0].strip()] = strs[1].strip()
        except Exception, e:
            raise e
        else:
            fopen.close()

    def has_key(self, key):
        return key in self.properties

    def get(self, key, default_value=''):
        if key in self.properties:
            return self.properties[key]
        return default_value

    def replace_property(self, file_name, from_regex, to_str, append_on_not_exists=True):
        tmpfile = tempfile.TemporaryFile()

        if os.path.exists(file_name):
            r_open = open(file_name, 'r')
            pattern = re.compile(r'' + from_regex)
            found = None
            for line in r_open:
                if pattern.search(line) and not line.strip().startswith('#'):
                    found = True
                    line = re.sub(from_regex, to_str, line)
                tmpfile.write(line)
            if not found and append_on_not_exists:
                tmpfile.write('\n' + to_str)
            r_open.close()
            tmpfile.seek(0)

            content = tmpfile.read()

            if os.path.exists(file_name):
                os.remove(file_name)

            w_open = open(file_name, 'w')
            w_open.write(content)
            w_open.close()

            tmpfile.close()
        else:
            print "file %s not found" % file_name

    def put(self, key, value):
        self.properties[key] = value
        self.replace_property(self.file_name, key + '=.*', key + '=' + value, True)

    def parse(self,file_name):
        return Properties(file_name)


# Function to get url
def get_url(prefix, project_id, suffix):
    url = prefix + project_id + suffix
    return url


# Generate a unique temp path
def gen_temp_dir_path(prefix):
    return prefix + "gatk_" + str(uuid.uuid1())
