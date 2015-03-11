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
@click.pass_context
def demultiplex(ctx):
	""" Demultiplex all runs present in the data directories
	"""
	an.run_demultiplexing()
