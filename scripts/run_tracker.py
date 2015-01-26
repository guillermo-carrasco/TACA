#!/usr/bin/env python
import argparse
import csv
import glob
import os
import shutil
import subprocess

import requests

from datetime import datetime

from pm.log import loggers
from pm.utils.filesystem import chdir
from pm.utils import config as cf
from pm.utils import parsers, misc

DESCRIPTION =(" Script to keep track and pre-process Illumina X Ten runs. "

"The script will work only with X Ten runs. X Ten runs generate different file "
"structure and naming than HiSeq 2000/2500. To run this script you will also need "
"bcl2fastq V >= 2.1."

"Once a run is completed and it has been pre-processed, demultiplexed data will be "
"sent to the processing server/HPC indicated in the configuration file.")

LOG = loggers.minimal_logger('Run tracker')

def check_config_options(config):
    """ Check that all needed configuration sections/config are present

    :param dict config: Parsed configuration file
    """
    try:
        config['preprocessing']
        config['preprocessing']['hiseq_data']
        config['preprocessing']['miseq_data']
        config['preprocessing']['mfs']
        config['preprocessing']['bcl2fastq']['path']
        config['preprocessing']['status_dir']
        config['preprocessing']['samplesheets_dir']
        config['preprocessing']['sync']
        config['preprocessing']['sync']['user']
        config['preprocessing']['sync']['host']
        config['preprocessing']['sync']['data_archive']
    except KeyError:
        raise RuntimeError(("Required configuration config not found, please "
            "refer to the README file."))


def is_finished(run):
    """ Checks if a run is finished or not. Check corresponding status file

    :param str run: Run directory
    """
    return os.path.exists(os.path.join(run, 'RTAComplete.txt'))


def processing_status(run):
    """ Returns the processing status of a sequencing run. Status are:

        TO_START - The BCL conversion and demultiplexing process has not yet started 
        IN_PROGRESS - The BCL conversion and demultiplexing process is started but not completed
        COMPLETED - The BCL conversion and demultiplexing process is completed

    :param str run: Run directory
    """
    demux_dir = os.path.join(run, 'Demultiplexing')
    if not os.path.exists(demux_dir):
        return 'TO_START'
    elif os.path.exists(os.path.join(demux_dir, 'Stats', 'DemultiplexingStats.xml')):
        return 'COMPLETED'
    else:
        return 'IN_PROGRESS'


def is_transferred(run, transfer_file):
    """ Checks wether a run has been transferred to the analysis server or not

    :param str run: Run directory
    :param str transfer_file: Path to file with information about transferred runs
    """
    try:
        with open(transfer_file, 'r') as f:
            t_f = csv.reader(f, delimiter='\t')
            for row in t_f:
                #Rows have two columns: run and transfer date
                if row[0] == os.path.basename(run):
                    return True
        if os.path.exists(os.path.join(run, 'transferring')):
            return True
        return False
    except IOError:
        return False


def transfer_run(run, config, analysis=True):
    """ Transfer a run to the analysis server. Will add group R/W permissions to
    the run directory in the destination server so that the run can be processed
    by any user/account in that group (i.e a functional account...)

    :param str run: Run directory
    :param dict config: Parsed configuration
    :param bool analysis: Trigger analysis on remote server
    """

    with chdir(run):
        cl = ['rsync', '-av']
        # Add R/W permissions to the group
        cl.append('--chmod=g+rw')
        # rsync works in a really funny way, if you don't understand this, refer to 
        # this note: http://silentorbit.com/notes/2013/08/rsync-by-extension/
        cl.append("--include=*/")
        for to_include in config['sync']['include']:
            cl.append("--include={}".format(to_include))
        cl.extend(["--exclude=*", "--prune-empty-dirs"])
        r_user = config['sync']['user']
        r_host = config['sync']['host']
        r_dir = config['sync']['data_archive']
        remote = "{}@{}:{}".format(r_user, r_host, r_dir)
        cl.extend([run, remote])

        # Create temp file indicating that the run is being transferred
        open('transferring', 'w').close()
        started = ("Started transfer of run {} on {}".format(os.path.basename(run), datetime.now()))
        LOG.info(started)
        # In this particular case we want to capture the exception because we want
        # to delete the transfer file
        try:
            misc.call_external_command(cl, with_log_files=True)
        except subprocess.CalledProcessError as e:
            os.remove('transferring')
            raise e

        t_file = os.path.join(config['status_dir'], 'transfer.tsv')
        LOG.info('Adding run {} to {}'.format(os.path.basename(run), t_file))
        with open(t_file, 'a') as tf:
            tsv_writer = csv.writer(tf, delimiter='\t')
            tsv_writer.writerow([os.path.basename(run), str(datetime.now())])
        os.remove('transferring')

        if analysis:
            trigger_analysis(run, config)


def trigger_analysis(run, config):
    """ Trigger the analysis of the flowcell in the analysis sever.

    :param str run: Run directory
    :param dict config: Parsed configuration
    """
    if not config.get('analysis'):
        LOG.warn(("No configuration found for remote analysis server. Not triggering"
                  "analysis of {}".format(os.path.basename(run))))
    else:
        url = "http://{}:{}/flowcell_analysis/{}".format(config['analysis']['host'],
                                                         config['analysis']['port'],
                                                         os.path.basename(run))
        try:
            r = requests.get(url)
            if r.status_code != requests.status_codes.codes.OK:
                LOG.warn(("Something went wrong when triggering the analysis of {}. Please "
                          "check the logfile and make sure to start the analysis!".format(os.path.basename(run))))
            else:
                LOG.info('Analysis of flowcell {} triggered in {}'.format(os.path.basename(run),
                                                                          config['analysis']['host']))
                a_file = os.path.join(config['status_dir'], 'analysis.tsv')
                with open(a_file, 'a') as af:
                    tsv_writer = csv.writer(af, delimiter='\t')
                    tsv_writer.writerow([os.path.basename(run), str(datetime.now())])
        except requests.exceptions.ConnectionError:
            LOG.warn(("Something went wrong when triggering the analysis of {}. Please "
                      "check the logfile and make sure to start the analysis!".format(os.path.basename(run))))


def run_bcl2fastq(run, config):
    """ Runs bcl2fast with the parameters found in the configuration file. After
    that, demultiplexed FASTQ files are sent to the analysis server.

    :param str run: Run directory
    :param dict config: Parset configuration file
    """
    LOG.info('Building bcl2fastq command')
    with chdir(run):
        cl_options = config['bcl2fastq']
        cl = [cl_options.get('path')]

        # Main options
        if cl_options.get('runfolder'):
            cl.extend(['--runfolder', cl_options.get('runfolder')])
        cl.extend(['--output-dir', cl_options.get('output-dir', 'Demultiplexing')])

        # Advanced options
        if cl_options.get('input-dir'):
            cl.extend(['--input-dir', cl_options.get('input-dir')])
        if cl_options.get('intensities-dir'):
            cl.extend(['--intensities-dir', cl_options.get('intensities-dir')])
        if cl_options.get('interop-dir'):
            cl.extend(['--interop-dir', cl_options.get('interop-dir')])
        if cl_options.get('stats-dir'):
            cl.extend(['--stats-dir', cl_options.get('stats-dir')])
        if cl_options.get('reports-dir'):
            cl.extend(['--reports-dir', cl_options.get('reports-dir')])

        # Processing cl_options
        threads = cl_options.get('loading-threads')
        if threads and type(threads) is int:
            cl.extend(['--loading-threads', threads])
        threads = cl_options.get('demultiplexing-threads')
        if threads and type(threads) is int:
            cl.extend(['--demultiplexing-threads', threads])
        threads = cl_options.get('processing-threads')
        if threads and type(threads) is int:
            cl.extend(['--processing-threads', threads])
        threads = cl_options.get('writing-threads')
        if threads and type(threads) is int:
            cl.extend(['--writing-threads', threads])

        # Behavioral options
        adapter_stringency = cl_options.get('adapter-stringency')
        if adapter_stringency and type(adapter_stringency) is float:
            cl.extend(['--adapter-stringency', adapter_stringency])
        aggregated_tiles = cl_options.get('aggregated-tiles')
        if aggregated_tiles and aggregated_tiles in ['AUTO', 'YES', 'NO']:
            cl.etend(['--aggregated-tiles', aggregated_tiles])
        barcode_missmatches = cl_options.get('barcode-missmatches')
        if barcode_missmatches and type(barcode_missmatches) is int \
                and barcode_missmatches in range(3):
            cl.extend(['--barcode-missmatches', barcode_missmatches])
        if cl_options.get('create-fastq-for-index-reads'):
            cl.append('--create-fastq-for-index-reads')
        if cl_options.get('ignore-missing-bcls'):
            cl.append('--ignore-missing-bcls')
        if cl_options.get('ignore-missing-filter'):
            cl.append('--ignore-missing-filter')
        if cl_options.get('ignore-missing-locs'):
            cl.append('--ignore-missing-locs')
        mask = cl_options.get('mask-short-adapter-reads')
        if mask and type(mask) is int:
            cl.extend(['--mask-short-adapter-reads', mask])
        minimum = cl_options.get('minimum-trimmed-reads')
        if minimum and type(minimum) is int:
            cl.extend(['--minimum-trimmed-reads', minimum])
        if cl_options.get('tiles'):
            cl.extend(['--tiles', cl_options.get('tiles')])

        if cl_options.get('use-bases-mask'):
            cl.extend(['--use-bases-mask', cl_options.get('use-bases-mask')])

        if cl_options.get('with-failed-reads'):
            cl.append('--with-failed-reads')
        if cl_options.get('write-fastq-reverse-complement'):
            cl.append('--write-fastq-reverse-complement')

        LOG.info(("BCL to FASTQ conversion and demultiplexing started for "
                  " run {} on {}".format(os.path.basename(run), datetime.now())))
        
        misc.call_external_command(cl, with_log_files=True)

        LOG.info(("BCL to FASTQ conversion and demultiplexing finished for "
                  "run {} on {}".format(os.path.basename(run), datetime.now())))
        # Transfer the processed data to the analysis server
        transfer_run(run, config)


if __name__=="__main__":
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('--config', type=str, help='Config file for the NGI pipeline')
    args = parser.parse_args()

    if not args.config:
        args.config = os.path.join(os.environ.get('HOME'), '.pm', 'pm.yaml')
    try:
        config = cf.load_yaml_config(args.config)
    except IOError as e:
        e.message = "No configuration file found in ~/.pm/pm.yaml or specified as parameter"
        raise e
    check_config_options(config)
    config = config['preprocessing']

    hiseq_runs = glob.glob(os.path.join(config['hiseq_data'], '1*XX'))
    for run in hiseq_runs:
        run_name = os.path.basename(run)
        LOG.info('Checking run {}'.format(run_name))
        if is_finished(run):
            status = processing_status(run)
            if  status == 'TO_START':
                LOG.info(("Starting BCL to FASTQ conversion and demultiplexing for "
                    "run {}".format(run_name)))
                run_bcl2fastq(run, config)
            elif status == 'IN_PROGRESS':
                LOG.info(("BCL conversion and demultiplexing process in progress for "
                    "run {}, skipping it".format(run_name)))
            elif status == 'COMPLETED':
                LOG.info(("Preprocessing of run {} is finished, check if run has been "
                    "transferred and transfer it otherwise".format(run_name)))

                t_file = os.path.join(config['status_dir'], 'transfer.tsv')
                transferred = is_transferred(run, t_file)
                if not transferred:
                    LOG.info("Run {} hasn't been transferred yet.".format(run_name))
                    LOG.info('Transferring run {} to {} into {}'.format(run_name,
                        config['sync']['host'],
                        config['sync']['data_archive']))
                    transfer_run(run, config)
                else:
                    LOG.info('Run {} already transferred to analysis server, skipping it'.format(run_name))

        if not is_finished(run):
            # Check status files and say i.e Run in second read, maybe something
            # even more specific like cycle or something
            LOG.info('Run {} is not finished yet'.format(run_name))