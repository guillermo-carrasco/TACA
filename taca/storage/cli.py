""" CLI for the storage subcommand
"""
import click
from taca.storage import storage as st


@click.group()
@click.option('-d', '--days', type=click.INT, default=None, help="Days to consider as thershold")
@click.option('-r', '--run', type=click.Path(exists=True))
@click.pass_context
def storage(ctx, days, run):
	""" Storage management methods and utilities """
	pass

# Storage subcommands
@storage.command()
@click.option('--backend', type=click.Choice(['swestore']), required=True,
              help='Long term storage backend')
@click.pass_context
def archive(ctx, backend):
    """ Archive old runs to SWESTORE
	"""
    params = ctx.parent.params
    if backend == 'swestore':
        st.archive_to_swestore(days=params.get('days'), run=params.get('run'))


@storage.command()
@click.option('--site', type=click.Choice(['swestore','archive','illumina','analysis','nas','proc']),
              required=True, help='Site to perform cleanup')
@click.option('-n','--dry-run', is_flag=True, help='Perform dry run i.e. Executes nothing but log')
@click.pass_context
def cleanup(ctx, site, dry_run):
    """ Do appropriate cleanup on the given site i.e. NAS/processing servers/UPPMAX """
    params = ctx.parent.params
    days = params.get('days')
    if not days:
        days = st.site_check_days[site]
    if site == 'nas':
#        st.cleanup_nas(days)
        pass
    if site == 'swestore':
        st.cleanup_swestore(days,dry_run)
        pass
