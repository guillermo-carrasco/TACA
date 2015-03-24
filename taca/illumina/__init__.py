""" Submodule with Illumina-related code
"""
import os

from xml.etree import ElementTree as ET

def demultiplex_HiSeq_X():
    """ Demultiplexing for HiSeq X runs
    """
    pass


def demultiplex_HiSeq():
    """ Demultiplexing for HiSeq (V3/V4) runs
    """
    pass


def demultiplex_MiSeq():
    """ Demultiplexing for MiSeq runs
    """
    pass


class Run(object):
    """ Defines an Illumina run
    """
    def __init__(self, run_dir):
        if not os.path.exists(run_dir) or not \
                os.path.exists(os.path.join(run_dir, 'runParameters.xml')):
            raise RuntimeError('Could not locate run directory {}'.format(run_dir))
        self.run_dir = run_dir
        self.id = os.path.basename(run_dir)
        self._extract_run_info()

        if self.run_type == 'HiSeq':
            self.demultiplex = demultiplex_HiSeq()
        elif self.run_type == 'HiSeq X':
            self.demultiplex = demultiplex_HiSeq_X()
        elif self.demultiplex == 'MiSeq':
            self.demultiplex = demultiplex_MiSeq()


    @property
    def is_finished(self):
        """ Returns true if the run is finished, false otherwise
        """
        return os.path.exists(os.path.join(self.run_dir, 'RTAComplete.txt'))


    def _extract_run_info(self):
        """ Extracts run info from runParameters.xml and adds it to the class attributes

        TODO: This method could be used to extract A LOT of information about the
        run and maybe... populate statusdb or similar? Just leaving this comment here
        till now...
        """

        run_parameters = ET.parse(os.path.join(self.run_dir, 'runParameters.xml')).getroot().find('Setup')
        # HiSeq and HiSeq X runParameter.xml files will have a Flowcell child with run type info
        run_type = run_parameters.find('Flowcell')
        # But MiSeqs doesn't...
        if not run_type:
            run_type = run_parameters.find('ApplicationName')

        try:
            if 'HiSeq X' in run_type.text:
                self.run_type = 'HiSeq X'
            elif 'HiSeq Flow Cell' in run_type.text:
                self.run_type = 'HiSeq'
            elif 'MiSeq' in run_type.text:
                self.run_type = 'MiSeq'
        except AttributeError:
            raise RuntimeError('Run type could not be determined for run {}'.format(self.id))
