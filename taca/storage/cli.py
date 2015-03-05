""" CLI for the storage subcommand
"""
import click



@click.group()
@click.option('-d', '--days', type=int, default=10,
		      help="Days to consider a run old")
@click.pass_context
def storage(ctx, days):
	""" Storage management methods and utilities """
	import ipdb; ipdb.set_trace()
	print "Storage called!"

# Storage subcommands
@storage.command()
def archive_to_swestore():
    """ Archive a run to swestore
    """
	# Here just call the actual archive-to-swestore method
    print "Archiving run!"


@storage.command()
def cleanup():
	""" Clean old runs from the data directories. """
	print "Cleaning runs!! "
	#same here, call the actual cleaning function
