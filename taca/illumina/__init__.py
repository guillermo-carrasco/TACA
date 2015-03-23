""" Submodule with Illumina-related code
"""
import os

from xml.etree import ElementTree as ET

def demultiplex_Hiseq_X():
    """ Demultiplexing for HiSeq X runs
    """
    pass


def demultiplex_Hiseq():
    """ Demultiplexing for HiSeq (V3/V4) runs
    """
    pass


def demultiplex_Miseq():
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
        run_info = self._extract_run_info()
        # XXX: Determine run type from runParameters.xml or wherever
        # XXX: Fill in common attributes (run id, maybe number of files, etc)
        # XXX: Instanciate correct run type


    @property
    def is_finished(self):
        """ Returns true if the run is finished, false otherwise
        """
        # XXX: Check RTAComplete.txt existence


    def _extract_run_info(self):
        """ Extract run info from runParameters.xml

        :returns dict info: Information about the run
        """
        run_info = {}
        run_parameters = ET.parse(os.path.join(self.run_dir, 'runParameters.xml'))
        
        return run_info
