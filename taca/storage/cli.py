""" CLI for the storage subcommand
"""

import click

import production


@click.group()
@click.option('-d', '--days', type=int, default=10,
		      help="Days to consider a run old")
@click.pass_context
def storage(context, days):
	""" Storage management methods and utilities """
	print context.params

for subcommand in production:
	storage.add_command(subcommand)
