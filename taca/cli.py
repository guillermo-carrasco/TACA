import os

import click

from pkg_resources import iter_entry_points

from taca.utils import config


@click.group()
# Priority for the configuration file is: environment variable > -c option > default
@click.option('-c', '--config-file',
			  default=os.path.join(os.environ['HOME'], '.taca/taca.yaml'),
			  envvar='TACA_CONFIG',
			  type=click.File('r'),
			  help='Path to TACA configuration file')
@click.pass_context
def cli(ctx, config_file):
	""" Tool for the Automation of Storage and Analyses """
	ctx.obj = {}
	ctx.obj['config'] = config.load_yaml_config(config_file)

#Add subcommands dynamically to the CLI
for entry_point in iter_entry_points('taca.subcommands'):
	cli.add_command(entry_point.load())
