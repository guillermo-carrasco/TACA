""" Divers utilities for Illumina runs processing """

import os
import xml.etree.ElementTree as ET

from taca.utils import parsers

def get_base_masks(rundir):
	""" Return a set of base masks to be used when demultiplexing.

	:param str rundir: Path to the run directory.
	:returns list: Basemasks to be used based on the SampleSeet information.
	"""

    runsetup = parsers.get_read_configuration(rundir, sort=True)
    flowcell_id = parsers.get_flowcell_id(rundir)
    base_masks = {}

    #Create groups of reads by index length
    ss_name = os.path.join(rundir, str(flowcell_id) + '.csv')
    if os.path.exists(ss_name):
        ss = csv.DictReader(open(ss_name, 'rb'), delimiter=',')
        samplesheet = []
        [samplesheet.append(read) for read in ss]
        for r in samplesheet:
            index_length = len(r['Index'].replace('-', '').replace('NoIndex', ''))
            if not base_masks.has_key(index_length):
                base_masks[index_length] = {'base_mask': [],
                                            'samples': {'fieldnames': ss.fieldnames, 'samples':[]}}
            base_masks[index_length]['samples']['samples'].append(r)

    #Create the basemask for each group
    for index_size, index_group in base_masks.iteritems():
        index_size = index_size
        group = index_size
        bm = []
        per_index_size = index_size/(int(_last_index_read(rundir)) - 1)

        for read in runsetup:
            cycles = read['NumCycles']
            if read['IsIndexedRead'] == 'N':
                bm.append('Y' + cycles)
            else:
                # I_iN_y(,I_iN_y) or I(,I)
                if index_size > int(cycles):
                    i_remainder = int(cycles) - per_index_size
                    if i_remainder > 0:
                        bm.append('I' + str(per_index_size) + 'N' + str(i_remainder))
                    else:
                        bm.append('I' + cycles)
                # I_iN_y(,N) or I(,N)
                else:
                    if index_size > 0:
                        to_mask = "I" + str(index_size)
                        if index_size < int(cycles):
                           to_mask = to_mask + 'N' + str(int(cycles) - index_size)
                        bm.append(to_mask)
                        index_size = 0
                    else:
                        bm.append('N' + cycles)
        base_masks[group]['base_mask'] = bm
    return base_masks
