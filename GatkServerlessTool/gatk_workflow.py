#!/usr/bin/env python
import sys
import json
import time
from util import Properties, format_path
from util import get_token
from util import get_url
from util import gen_temp_dir_path
from util import get_in_output
import logging.config
import urllib2
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# log config
logging.config.fileConfig('logging.config')
# output in log file
log = logging.getLogger('log')

file_path = 'config.property'
props = Properties(file_path)
ak = props.get('access_key')
sk = props.get('secret_key')

# get input path and output path from command line;
input_path1, output_path, type, picard_args = get_in_output(sys.argv[1:], ak, sk)

# Read config.prpperty
iam_endpoint = props.get('iam_endpoint')
dli_endpoint = props.get('dli_endpoint')
user_name = props.get('user_name')
password = props.get('password')
project_id = props.get('project_id')
cluster_name = props.get('cluster_name')
jar_file = props.get('file', 'gatk-package-4.0.4.0-37.jar')
# todo: ref and knownsites should use HDFS path
ref_path = format_path(props.get('ref_path'), ak, sk)
knownsite_paths_unformat = props.get('knownsite_paths').split(",")
knownsite_paths = []
for known_site in knownsite_paths_unformat:
    knownsite_paths.append(format_path(known_site, ak, sk))
temp_dir_prefix = props.get('temp_dir_prefix', 'hdfs://hacluster/user/mls/gatk/')
task_status_check_period = float(props.get('task_status_check_period', 5))
partition_size = int(props.get('partition_size', 34217728))

# Temp dir info in hdfs
file_name_with_ext = output_path[output_path.rindex('/') + 1:]
file_name = file_name_with_ext[0:file_name_with_ext.rindex('.')]
temp_dir = gen_temp_dir_path(temp_dir_prefix)
mark_duplicates_output_path = temp_dir + '/' + file_name + '.markdup.bam'
mark_duplicates_metric_path = temp_dir + '/' + file_name + '.markdup.metrics.txt'
bqsr_pipeline_output_path = temp_dir + '/' + file_name + '.markdup.bgsr.bam'
vcf_output_path = temp_dir + '/' + file_name + '.vcf'
ubam_path = ''
bam_path = ''

# Dli-picard related conf
dli_picard_jar = props.get('picard_file', 'dli-picard-1.1.jar')
picard_cluster_name = props.get('picard_cluster_name')

# Get token
token = get_token(iam_endpoint, user_name, password, project_id)
headers = {'Content-Type': 'application/json', 'X-Auth-Token': token}

# Common batch data, used for BwaSpark and ReadsPipelineSpark
data = {
    'file': jar_file,
    'className': 'org.broadinstitute.hellbender.Main',
    'cluster_name': cluster_name,
    "modules": ["sys.dli.gatk"],
    'sc_type': 'C',
    'conf': {
        "spark.driver.userClassPathFirst": "false",
        "spark.io.compression.codec": "lzf",
        "spark.driver.maxResultSize": "0",
        "spark.kryoserializer.buffer.max": "512m",
        "spark.yarn.executor.memoryOverhead": "13312",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        "spark.dynamicAllocation.executorIdleTimeout": "300s",
        "spark.dynamicAllocation.cachedExecutorIdleTimeout": "180000000",
        "spark.dynamicAllocation.enabled": "false",
    },
    'driverMemory':'22g',
    'driverCores':'4',
    'executorMemory':'12g',
    'executorCores':'4',
    'numExecutors': '63'
}


# Data used only for dli-picard
picard_data = {
    'file': dli_picard_jar,
    'className': 'com.huawei.code.FastQToBam',
    'cluster_name': picard_cluster_name,
    'sc_type': 'C',
    "modules": ["sys.dli.gatk"],
    'conf': {
        "spark.driver.userClassPathFirst": "false",
        "spark.io.compression.codec": "lzf",
        "spark.driver.maxResultSize": "0",
        "spark.kryoserializer.buffer.max": "512m",
        "spark.yarn.executor.memoryOverhead": "1024",
        "spark.yarn.driver.memoryOverhead": "1024",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    },
    'driverMemory': '18g',
    'driverCores': '4',
    'executorMemory': '8g',
    'executorCores': '4'
}


def submit_task(task_name, payload):
    # submit task
    suffix = '/batches'
    batch_url = get_url(dli_endpoint, project_id, suffix)
    req = urllib2.Request(batch_url, data=json.dumps(payload), headers=headers)
    log.info('The body of request is {}'.format(req.data))
    resp = urllib2.urlopen(req)
    batch = json.loads(resp.read())
    assert batch != [], 'Submit task {name} failed!'.format(name=task_name)
    log.debug(batch)
    batch_id = str(batch['id'])
    log.info('Submit task {name} success!'.format(name=task_name))

    # wait task finished
    result = wait_task_finish(batch_id)
    if result:
        log.info('Task {name} successful finished.'.format(name=task_name))
    else:
        log.error('Failed to process {name}, thus exit.'.format(name=task_name))
        sys.exit(-1)


def wait_task_finish(task_id):
    start_time = time.time()
    is_finished = False
    result = False
    while not is_finished:
        time.sleep(task_status_check_period)
        is_finished, result = check_task_status(task_id, start_time)
    return result


def check_task_status(task_id, start_time):
    suffix = '/batches/{batch_id}'.format(batch_id=task_id)
    batch_url = get_url(dli_endpoint, project_id, suffix)
    req = urllib2.Request(batch_url, headers=headers)
    resp = urllib2.urlopen(req)
    log.info('status code is {}'.format(resp.getcode()))
    batch = json.loads(resp.read())
    assert batch != [], 'Get task status failed!'
    log.debug(batch)
    elapsed = time.time() - start_time
    status = str(batch['state'])
    if status == 'success':
        log.info('Task {task_id} finished with result success, total cost {elapsed} seconds.'.format(
            task_id=task_id, elapsed=elapsed))
        return True, True
    elif status == 'dead':
        log.info('Task {task_id} finished with result failed, total cost {elapsed} seconds.'.format(
            task_id=task_id, elapsed=elapsed))
        return True, False
    else:
        log.info('Task {task_id} has running {elapsed} seconds, current status is {status}.'.format(
            task_id=task_id, elapsed=elapsed, status=status))
        return False, True


# First Step: MarkDuplicates
def mark_duplicates():
    data['name'] = 'MarkDuplicatesSpark'
    data['args'] = [
        "MarkDuplicatesSpark",
        "-I", bam_path,
        "-O", mark_duplicates_output_path,
        "-M", mark_duplicates_metric_path,
        "--spark-master", "yarn-cluster",
        "--num-reducers", "1500",
        "--bam-partition-size", "64217728",
        "--sharded-output", "true"
    ]
    submit_task("MarkDuplicatesSpark", data)


# Second Step: BaseRecalibrator & ApplyBQSR
def bqsr_pipeline():
    data['name'] = 'BQSRPipelineSpark'
    data['args'] = [
        "BQSRPipelineSpark",
        "-I", mark_duplicates_output_path,
        "-O", bqsr_pipeline_output_path,
        "-R", ref_path,
        "--spark-master", "yarn-cluster",
        "--num-reducers", "1500",
        "--bam-partition-size", "64217728",
        "--sharded-output", "true"
    ]
    for path in knownsite_paths:
        data['args'].append("--known-sites")
        data['args'].append(path)

    submit_task("BQSRPipelineSpark", data)


# Third Step: HaplotypeCaller
def haplotype_caller():
    data['name'] = 'HaplotypeCallerSpark'
    data['args'] = [
        "HaplotypeCallerSpark",
        "-I", bqsr_pipeline_output_path,
        "-O", vcf_output_path,
        "-R", ref_path,
        "--spark-master", "yarn-cluster",
        "--num-reducers", "3000",
        "--bam-partition-size", "64217728"
    ]
    submit_task("HaplotypeCallerSpark", data)


# Only one Step: ReadsPipelineSpark
def reads_pipeline_spark():
    data['name'] = 'ReadsPipelineSpark'
    data['args'] = [
        "ReadsPipelineSpark",
        "-I", bam_path,
        "-O", vcf_output_path,
        "-R", ref_path,
        "--spark-master", "yarn-cluster",
        "--num-reducers", "8000",
    ]
    data['driverMemory'] = '22g'
    data['driverCores'] = '4'
    data['executorMemory'] = '23g'
    data['executorCores'] = '4'
    data['numExecutors'] = '63'
    data['conf'] = {
        "spark.driver.userClassPathFirst": "false",
        "spark.io.compression.codec": "lzf",
        "spark.driver.maxResultSize": "0",
        "spark.kryoserializer.buffer.max": "512m",
        "spark.yarn.executor.memoryOverhead": "2048",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        "spark.dynamicAllocation.cachedExecutorIdleTimeout": "180000000",
        "spark.dynamicAllocation.enabled": "false",
        }

    for path in knownsite_paths:
        data['args'].append("--known-sites")
        data['args'].append(path)

    submit_task("ReadsPipelineSpark", data)


def export_to_obs():
    payload = {
        'file': "luxor-hdfs-tool-0.1.0.jar",
        'className': 'com.huawei.luxor.hdfs.FileSystemOperator',
        'cluster_name': cluster_name,
        'name': 'exportToObs',
        'modules': ['sys.dli.gatk'],
        'args': [
            "export",
            "-I",
            vcf_output_path,
            "-O",
            output_path
        ]
    }

    submit_task("exportToObs", payload)


# Final Step: clear temp data
def clear_temp_data():
    payload = {
        'file': "luxor-hdfs-tool-0.1.0.jar",
        'className': 'com.huawei.luxor.hdfs.FileSystemOperator',
        'cluster_name': cluster_name,
        'name': 'CleanData',
        'modules': ['sys.dli.gatk'],
        'args': [
            "rmr",
            "-P",
            temp_dir
        ]
    }
    batch_url = get_url(dli_endpoint, project_id, '/batches')
    req = urllib2.Request(batch_url, data=json.dumps(payload), headers=headers)
    log.info('The body of request is {}'.format(req.data))
    resp = urllib2.urlopen(req)
    log.info('status code is {}'.format(resp.getcode()))
    log.debug(resp.read())


def fastq_to_ubam():
    picard_data['name'] = 'FastQToUBam'
    picard_data['args'] = [
        "FastQToBam",
        "O=" + ubam_path
    ] + picard_args
    submit_task("FastQToUBam", picard_data)


def ubam_to_bam():
    data['name'] = 'BwaSpark'
    data['args'] = [
        "BwaSpark",
        "-I", ubam_path,
        "-O", bam_path,
        "-R", ref_path,
        "--spark-master", "yarn-cluster",
        "--num-reducers", "8000",
        "--sharded-output", "true"
    ]
    submit_task("BwaSpark", data)


if __name__ == '__main__':
    log.info('########## GATK PROCESS JOB STARTING ##########')
    s_time = time.time()
    try:

        if type.lower() == 'bam':
            bam_path = input_path1
        elif type.lower() == 'ubam':
            ubam_path = input_path1
            log.info('Starting running step uBamToBam')
            ubam_to_bam()
            log.info('Finished running step uBamToBam')
        elif type.lower() == 'fastq':
            bam_path = temp_dir + '/bam/'
            ubam_path = temp_dir + '/ubam/'
            log.info('Starting running step FastQToUBam')
            fastq_to_ubam()
            log.info('Finished running step FastQToUBam')
            log.info('Starting running step uBamToBam')
            ubam_to_bam()
            log.info('Finished running step uBamToBam')
        else:
            log.error('Only support fastq or bam as input file type')
            sys.exit()

        log.info('Starting running step ReadsPipelineSpark')
        reads_pipeline_spark()
        log.info('Finished running step ReadsPipelineSpark')
        export_to_obs()
        log.info("Finished export VCF file.")
    finally:
        log.info('Starting running step cleantempdata')
        clear_temp_data()
        log.info('Finished running step cleantempdata')
    e_time  = time.time()
    duration = e_time - s_time
    log.info('########## GATK JOB FINISHED SUCCESSFULLY, total cost {} seconds ##########'.format(duration))

