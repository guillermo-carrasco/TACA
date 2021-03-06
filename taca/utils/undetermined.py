import re
import subprocess
import gzip
import glob
import os
import logging
import flowcell_parser.classes as cl
from taca.utils.config import CONFIG

logger=logging.getLogger(__name__)
dmux_folder='Demultiplexing'

def check_undetermined_status(run, und_tresh=10, q30_tresh=75, freq_tresh=40, pooled_tresh=5, dex_status='COMPLETED'):
    """Will check for undetermined fastq files, and perform the linking to the sample folder if the
    quality thresholds are met.

    :param run: path of the flowcell
    :type run: str
    :param und_tresh: max percentage of undetermined indexed in a lane allowed
    :type und_tresh: float
    :param q30_tresh: lowest percentage of q30 bases allowed
    :type q30_tresh: float
    :param freq_tresh: highest allowed percentage of the most common undetermined index
    :type freq_tresh: float:w

    :returns boolean: True  if the flowcell passes the checks, False otherwise
    """
    global dmux_folder
    try:
        dmux_folder=CONFIG['analysis']['bcl2fastq']['options'][0]['output_dir']
    except KeyError:
        dmux_folder='Demultiplexing'

    status=False
    if os.path.exists(os.path.join(run, dmux_folder)):
        xtp=cl.XTenParser(run)
        ss=xtp.samplesheet
        lb=xtp.lanebarcodes
        path_per_lane=get_path_per_lane(run, ss)
        samples_per_lane=get_samples_per_lane(ss)
        workable_lanes=get_workable_lanes(run, dex_status)
        for lane in workable_lanes:
            if is_unpooled_lane(ss,lane):
                rename_undet(run, lane, samples_per_lane)
                if check_index_freq(run,lane, freq_tresh):
                    if lb :
                        if first_qc_check(lane,lb, und_tresh, q30_tresh):
                            link_undet_to_sample(run, lane, path_per_lane)
                            status=True
                        else:
                            logger.warn("lane {} did not pass the qc checks, the Undetermined will not be added.".format(lane))
                            status=False
                    else:
                        logger.info("The HTML report is not available yet, will wait.")
                else:
                    logger.warn("lane {} did not pass the qc checks, the Undetermined will not be added.".format(lane))
                    status=False
            else:
                if lb and qc_for_pooled_lane(lane,lb,pooled_tresh):
                    return True
                logger.warn("The lane {}  has been multiplexed, according to the samplesheet and will be skipped.".format(lane))
    else:
        logger.warn("No demultiplexing folder found, aborting")

    return status

def qc_for_pooled_lane(lane,lb , und_thresh):
    d={}
    for entry in lb.sample_data:
        if lane == int(entry['Lane']):
            if entry.get('Sample')!='unknown':
                d['det']=int(entry['Clusters'].replace(',',''))
            else:
                d['undet']=int(entry['Clusters'].replace(',',''))


    if d['undet'] > (d['det']+d['undet']) * und_thresh / 100:
        logger.warn("Lane {} has more than {}% undetermined indexes ({}%)".format(lane, und_thresh,d['undet']/(d['det']+d['undet'])*100))
        return False

    return True

    


def rename_undet(run, lane, samples_per_lane):
    """Renames the Undetermined fastq file by prepending the sample name in front of it

    :param run: the path to the run folder
    :type run: str
    :param status: the demultiplexing status
    :type status: str
    :param samples_per_lane: lane:sample dict
    :type status: dict
    """
    for file in glob.glob(os.path.join(run, dmux_folder, "Undetermined*L0?{}*".format(lane))):
        old_name=os.path.basename(file)
        old_name_comps=old_name.split("_")
        old_name_comps[1]=old_name_comps[0]# replace S0 with Undetermined
        old_name_comps[0]=samples_per_lane[lane]#replace Undetermined with samplename
        for index, comp in enumerate(old_name_comps):
            if comp.startswith('L00'):
                old_name_comps[index]=comp.replace('L00','L01')#adds a 1 as the second lane number in order to differentiate undetermined from normal in piper
                
        new_name="_".join(old_name_comps)
        logger.info("Renaming {} to {}".format(file, os.path.join(os.path.dirname(file), new_name)))
        os.rename(file, os.path.join(os.path.dirname(file), new_name))
def get_workable_lanes(run, status):
    """List the lanes that have a .fastq file

    :param run: the path to the run folder
    :type run: str
    :param status: the demultiplexing status
    :type status: str

    :rtype: list of ints 
    :returns:: list of lanes having an undetermined fastq file
    """
    lanes=[]
    pattern=re.compile('L0[0,1]([0-9])')
    for unde in glob.glob(os.path.join(run, dmux_folder, '*Undetermined_*')):
        name=os.path.basename(unde)
        lanes.append(int(pattern.search(name).group(1)))
    lanes=list(set(lanes))
    if status =='IN_PROGRESS': 
        #the last lane is the one that is currently being worked on by bcl2fastq, don't work on it.
        lanes=lanes[:-1]
    logger.info("post_demux processing will happen with lanes {}".format(lanes))
    return lanes


def link_undet_to_sample(run, lane, path_per_lane):
    """symlinks the undetermined file to the right sample folder with a RELATIVE path so it's carried over by rsync
    
    :param run: path of the flowcell
    :type run: str
    :param lane: lane identifier
    :type lane: int
    :param path_per_lane: {lane:path/to/the/sample}
    :type path_per_lane: dict"""
    for fastqfile in glob.glob(os.path.join(run, dmux_folder, '*Undetermined*_L0?{}_*'.format(lane))):
        if not os.path.exists(os.path.join(path_per_lane[lane], os.path.basename(fastqfile))):
            fqbname=os.path.basename(fastqfile)
            logger.info("linking file {} to {}".format(fastqfile, path_per_lane[lane]))
            os.symlink(os.path.join('..','..',fqbname), os.path.join(path_per_lane[lane], os.path.basename(fastqfile)))

def save_index_count(barcodes, run, lane):
    """writes the barcode counts

    :param barcodes: {barcode:count}
    :type barcodes: dict
    :param run: path to the flowcell
    :type run: str
    """
    with open(os.path.join(run, dmux_folder, 'index_count_L{}.tsv'.format(lane)), 'w') as f:
        for barcode in sorted(barcodes, key=barcodes.get, reverse=True):
            f.write("{}\t{}\n".format(barcode, barcodes[barcode]))

def check_index_freq(run, lane, freq_tresh):
    """uses subprocess to perform zcat <file> | sed -n '1~4 p' | awk -F ':' '{print $NF}', counts the barcodes and 
    returns true if the most represented index accounts for less than freq_tresh% of the total
    
    :param run: path to the flowcell
    :type run: str
    :param lane: lane identifier
    :type lane: int
    :param freq_tresh: maximal allowed frequency of the most frequent undetermined index
    :type frew_tresh: float
    :rtype: boolean
    :returns: True if the checks passes, False otherwise
    """
    barcodes={}
    if os.path.exists(os.path.join(run, dmux_folder,'index_count_L{}.tsv'.format(lane))):
        logger.info("Found index count for lane {}.".format(lane))
        with open(os.path.join(run, dmux_folder,'index_count_L{}.tsv'.format(lane))) as idxf:
            for line in idxf:
                barcodes[line.split('\t')[0]]=int(line.split('\t')[1])

    else:
        open(os.path.join(run, dmux_folder,'index_count_L{}.tsv'.format(lane)), 'a').close()
        for fastqfile in glob.glob(os.path.join(run, dmux_folder, '*Undetermined*_L0?{}_R1*'.format(lane))):
            logger.info("working on {}".format(fastqfile))
            zcat=subprocess.Popen(['zcat', fastqfile], stdout=subprocess.PIPE)
            sed=subprocess.Popen(['sed', '-n', "1~4p"],stdout=subprocess.PIPE, stdin=zcat.stdout)
            awk=subprocess.Popen(['awk', '-F', ":", '{print $NF}'],stdout=subprocess.PIPE, stdin=sed.stdout)
            zcat.stdout.close()
            sed.stdout.close()
            output = awk.communicate()[0]
            zcat.wait()
            sed.wait()
            for barcode in output.split('\n')[:-1]:
                try:
                    barcodes[barcode]=barcodes[barcode]+1
                except KeyError:
                    barcodes[barcode]=1

        save_index_count(barcodes, run, lane)
    total=sum(barcodes.values())
    count, bar = max((v, k) for k, v in barcodes.items())
    if total * freq_tresh / 100<count:
        logger.warn("The most frequent barcode of lane {} ({}) represents {}%, "
                "which is over the threshold of {}%".format(lane, bar, count * 100 / total , freq_tresh))
        return False
    else:
        logger.info("Most frequent undetermined index represents less than {}% of the total, lane {} looks fine.".format(freq_tresh, lane))
        return True




def first_qc_check(lane, lb, und_tresh, q30_tresh):
    """checks wether the percentage of bases over q30 for the sample is 
    above the treshold, and if the amount of undetermined is below the treshold
    
    :param lane: lane identifier
    :type lane: int
    :param lb: reader of laneBarcodes.html
    :type lb: flowcell_parser.classes.XTenLaneBarcodes
    :param und_tresh: maximal allowed percentage of undetermined indexes
    :type und_tresh: float
    :param q30_tresh: maximal allowed  percentage of bases over q30
    :type q30_tresh: float

    :rtype: boolean
    :returns: True of the qc checks pass, False otherwise
    
    """
    d={}
    for entry in lb.sample_data:
        if lane == int(entry['Lane']):
            if entry.get('Sample')!='unknown':
                if float(entry['% >= Q30bases']) < q30_tresh:
                    logger.warn("Sample {} of lane {} has a percentage of bases over q30 of {}%, "
                            "which is below the cutoff of {}% ".format(entry['Sample'], lane, float(entry['% >= Q30bases']), q30_tresh))
                    return False
                d['det']=int(entry['Clusters'].replace(',',''))
            else:
                d['undet']=int(entry['Clusters'].replace(',',''))


    if d['undet'] > (d['det']+d['undet']) * und_tresh / 100:
        logger.warn("Lane {} has more than {}% undetermined indexes ({:.2f}%)".format(lane, und_tresh,float(d['undet'])/(d['det']+d['undet'])*100))
        return False

    return True




def get_path_per_lane(run, ss):
    """
    :param run: the path to the flowcell
    :type run: str
    :param ss: SampleSheet reader
    :type ss: flowcell_parser.XTenSampleSheet
    """
    d={}
    for l in ss.data:
        try:
            d[int(l['Lane'])]=os.path.join(run, dmux_folder, l['Project'], l['SampleID'])
        except KeyError:
            logger.error("Can't find the path to the sample, is 'Project' in the samplesheet ?")
            d[int(l['Lane'])]=os.path.join(run, dmux_folder)

    return d
def get_samples_per_lane(ss):
    """
    :param ss: SampleSheet reader
    :type ss: flowcell_parser.XTenSampleSheet
    :rtype: dict
    :returns: dictionnary of lane:samplename
    """
    d={}
    for l in ss.data:
        s=l['SampleName'].replace("Sample_", "").replace("-", "_")
        d[int(l['Lane'])]=l['SampleName']

    return d

def get_barcode_per_lane(ss):
    """
    :param ss: SampleSheet reader
    :type ss: flowcell_parser.XTenSampleSheet
    :rtype: dict
    :returns: dictionnary of lane:barcode
    """
    d={}
    for l in ss.data:
        d[int(l['Lane'])]=l['index']

    return d


def is_unpooled_lane(ss, lane):
    """
    :param ss: SampleSheet reader
    :type ss: flowcell_parser.XTenSampleSheet
    :param lane: lane identifier
    :type lane: int
    :rtype: boolean
    :returns: True if the samplesheet has one entry for that lane, False otherwise
    """
    count=0
    for l in ss.data:
        if int(l['Lane']) == lane:
            count+=1
    return count==1

def is_unpooled_run(ss):
    """
    :param ss: SampleSheet reader
    :type ss: flowcell_parser.XTenSampleSheet
    :rtype: boolean
    :returns: True if the samplesheet has one entry per lane, False otherwise
    """
    ar=[]
    for l in ss.data:
        ar.append(l['Lane'])
    return len(ar)==len(set(ar))

        

if __name__=="__main__":
    import sys

    mainlog = logging.getLogger(__name__)
    mainlog.setLevel(level=logging.INFO)
    mfh = logging.StreamHandler(sys.stderr)
    mft = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    mfh.setFormatter(mft)
    mainlog.addHandler(mfh)
        
    check_undetermined_status("/srv/illumina/HiSeq_X_data/150424_ST-E00214_0031_BH2WY7CCXX")
