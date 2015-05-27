""" CLI for the analysis subcommand
"""
import click
from taca.analysis import analysis as an


@click.group()
def analysis():
	""" Analysis methods entry point """
	pass

# analysis subcommands
@analysis.command()
@click.option('-r', '--run', type=click.Path(exists=True), default=None,
				 help='Demultiplex only a particular run')
def demultiplex(run):
	""" Demultiplex all runs present in the data directories
	"""
	an.run_preprocessing(run)

@analysis.command()
@click.argument('rundir')
def transfer(rundir):
    """Transfers the run without qc"""
    an.transfer_run(rundir)
