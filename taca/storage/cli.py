""" CLI for the storage subcommand
"""
import click
from taca.storage import storage as st


@click.group()
@click.option('-d', '--days', type=int, default=2,
		      help="Days to consider a run old")
@click.option('-r', '--run', type=click.Path(exists=True))
@click.pass_context
def storage(ctx, days, run):
	""" Storage management methods and utilities """
	pass

# Storage subcommands
@storage.command()
@click.pass_context
def archive_to_swestore(ctx):
    """ Archive old runs to SWESTORE
    """
	# Here just call the actual archive-to-swestore method
    raise NotImplementedError('Sorry...')


@storage.command()
@click.pass_context
def cleanup(ctx):
	""" Move old runs to nosync directory so they're not synced to the processing server """
	params = ctx.parent.params
	st.cleanup(ctx.obj['config'], ctx.obj['logger'], days=params.get('days'), run=params.get('run'))
