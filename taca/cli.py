import os

import click

from pkg_resources import iter_entry_points

# Here add all common configuration options for all the subcommands in TACA, i.e run
@click.group()
@click.option('-c', '--config',
			  default=os.path.join(os.environ['HOME'], '.taca/taca.yaml'),
			  type=click.File('r'),
			  help='Path to TACA configuration file')
@click.option('-r', '--run', type=click.Path(exists=True),
              help='Path to a specific run')
@click.pass_context
def cli(context, config, run):
	""" Tool for the Automation of Storage and Analyses """

#Add subcommands dynamically to the CLI
for entry_point in iter_entry_points('taca.subcommands'):
	cli.add_command(entry_point.load())
