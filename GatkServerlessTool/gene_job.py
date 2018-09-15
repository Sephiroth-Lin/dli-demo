#!/usr/bin/env python

import sys
import getopt
import json
import time
from util import Properties
from util import get_token
from util import get_url
import logging.config
import urllib2
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# log config
logging.config.fileConfig('logging.config')
# output in log file
log = logging.getLogger('log')

# Read config.prpperty
file_path = 'gene_config.property'
props = Properties(file_path)
iam_endpoint = props.get('iam_endpoint')
dli_endpoint = props.get('dli_endpoint')
user_name = props.get('user_name')
password = props.get('password')
project_id = props.get('project_id')
job_status_check_period = float(props.get('job_status_check_period', 300))

file_type = props.get('file_type', '')
ref = props.get('ref', '')
known_sites = props.get('known_sites', '')
conf = props.get('conf', '')
is_export_bam = props.get('is_export_bam', 'False')
bam_output_path = props.get('bam_output_path', '')
is_output_gvcf = props.get('is_output_gvcf', 'False')

gene_job_url = get_url(dli_endpoint, project_id, '/gene/jobs')

headers = ''

# usage
usage = 'USAGE: <program> args \n\n' \
        'Valid programs and its args:\n' \
        '  SubmitJob:\n' \
        '    -I input obs paths, separate with common(,)\n' \
        '    -O output obs path\n' \
        '  ListJob\n'


def submit_job(payload):
    # submit task
    try:
        req = urllib2.Request(gene_job_url, data=json.dumps(payload), headers=headers)
        log.info('The body of request is {}'.format(req.data))
        resp = urllib2.urlopen(req)
        job = json.loads(resp.read())
        assert job != [], 'Submit job failed!'
        log.debug(job)
        job_id = str(job['job_id'])
        log.info('Submit job: {job_id} success!'.format(job_id=job_id))

        # wait job finished
        wait_job_finish(job_id)
    except urllib2.HTTPError as e:
        error_msg = json.dumps(json.loads(e.read()), indent=4)
        log.error("Failed to submit job: \n" + error_msg)
        raise e


def list_jobs():
    req = urllib2.Request(gene_job_url, headers=headers)
    resp = urllib2.urlopen(req)
    return resp.read()


def wait_job_finish(job_id):
    start_time = time.time()
    is_finished = False
    result = False
    while not is_finished:
        time.sleep(job_status_check_period)
        is_finished, result = check_job_status(job_id, start_time)
    return result


def check_job_status(job_id, start_time):
    jobs_resp = json.loads(list_jobs())
    jobs = jobs_resp['jobs']
    target_job = None
    for job in jobs:
        if job['job_id'] == job_id:
            target_job = job
            break

    elapsed = time.time() - start_time
    status = str(target_job['status'])
    progress = str(target_job['progress'])
    if status == 'FINISHED':
        log.info('Job {job_id} finished with result success, total cost {elapsed} seconds.'.format(
            job_id=job_id, elapsed=elapsed))
        return True, True
    elif status == 'FAILED':
        log.info('Job {job_id} finished with result failed, total cost {elapsed} seconds.'.format(
            job_id=job_id, elapsed=elapsed))
        return True, False
    elif status == 'CANCELLED':
        log.info('Job {job_id} was canceled, total cost {elapsed} seconds.'.format(
            job_id=job_id, elapsed=elapsed))
        return True, False
    else:
        log.info('Job {job_id} has running {elapsed} seconds, current status is {status}, '
                 'progress is {progress}.'.format(job_id=job_id, elapsed=elapsed, status=status, progress=progress))
        return False, True


def parse_conf():
    equal_format_confs = []
    if conf != '':
        colon_format_confs = conf.split(',')
        for colon_format_conf in colon_format_confs:
            key_value_pair = colon_format_conf.strip(' ').split(':')
            equal_format_confs.append('{key}={value}'.format(
                key=key_value_pair[0].strip(' '), value=key_value_pair[1].strip(' ')))
    return equal_format_confs


def parse_args(argv):

    try:
        opts, args = getopt.getopt(argv, 'hI:O:', ['help'])
    except getopt.GetoptError:
        print usage
        sys.exit(2)

    parsed_args = {'input_paths': '', 'output_path': ''}
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print usage
            sys.exit()
        elif opt == '-I':
            parsed_args['input_paths'] = arg
        elif opt == '-O':
            parsed_args['output_path'] = arg
        else:
            log.info("Unknown option: {option}".format(option=opt))

    if parsed_args['input_paths'] == '' or parsed_args['output_path'] == '':
        print usage
        sys.exit()
    return parsed_args


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print usage
        sys.exit()

    program = sys.argv[1]
    if program == 'SubmitJob':
        args = parse_args(sys.argv[2:])
        if file_type == '' or ref == '' or known_sites == '':
            print "file_type, ref and known_sites should be set in gene_config.property file."
            sys.exit()
        payload = {
            'job_type': 'DNA',
            'input': args['input_paths'].split(','),
            'output': args['output_path'],
            'file_type': file_type,
            'ref': ref,
            'knownsites': known_sites.split(','),
            'conf': parse_conf(),
            'is_export_bam': is_export_bam.lower() == 'true',
            'is_output_gvcf': is_output_gvcf.lower() == 'true'
        }
        if bam_output_path != '':
            payload['bam_output'] = bam_output_path

        log.info('########## START TO PROCESS GENE JOB ##########')
        s_time = time.time()
        # 1. get token
        token = get_token(iam_endpoint, user_name, password, project_id)
        headers = {'Content-Type': 'application/json', 'X-Auth-Token': token}
        # 2. submit job
        submit_job(payload)
        e_time = time.time()
        duration = e_time - s_time
        log.info('########## GATK JOB FINISHED, total cost {} seconds ##########'.format(duration))
    elif program == 'ListJob':
        log.info('Trying to list all gene jobs:')
        # 1. get token
        token = get_token(iam_endpoint, user_name, password, project_id)
        headers = {'Content-Type': 'application/json', 'X-Auth-Token': token}
        # 2. list jobs
        print json.dumps(json.loads(list_jobs()), indent=4)
    else:
        print usage
