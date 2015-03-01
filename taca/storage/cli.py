""" CLI for the storage subcommand
"""

import click

@click.group()
@click.option('-d', '--days', type=int, default=10,
		      help="Days to consider a run old")
def storage(context):
	""" Storage management methods and utilities """
	pass
