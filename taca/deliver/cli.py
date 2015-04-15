""" CLI for the deliver subcommand
"""
import click
from taca.utils.misc import send_mail
from taca.deliver import deliver as _deliver

@click.group()
@click.pass_context
@click.option('--deliverypath', type=click.STRING,
			  help="Deliver to this destination folder")
@click.option('--stagingpath', type=click.STRING,
			  help="Stage the delivery under this path")
@click.option('--uppnexid', type=click.STRING,
			  help="Use this UppnexID instead of fetching from database")
@click.option('--operator', type=click.STRING, default=None, multiple=True,
			  help="Email address to notify operator at. Multiple operators can be specified")
@click.option('--stage_only', is_flag=True, default=False,
			  help="Only stage the delivery but do not transfer any files")
def deliver(ctx,deliverypath,stagingpath,uppnexid,operator,stage_only):
    """ Deliver methods entry point
    """
    if deliverypath is None:
        del ctx.params['deliverypath']
    if stagingpath is None:
        del ctx.params['stagingpath']
    if uppnexid is None:
        del ctx.params['uppnexid']
    if operator is None or len(operator) == 0:
        del ctx.params['operator']
    
# deliver subcommands
        
## project delivery
@deliver.command()
@click.pass_context
@click.argument('projectid',type=click.STRING,nargs=1)
def project(ctx, projectid):
    """ Deliver the specified project to the specified destination
    """
    d = _deliver.ProjectDeliverer(
        projectid,
        **ctx.parent.params)
    _exec_delivery(d,d.deliver_project)
    
## sample delivery
@deliver.command()
@click.pass_context
@click.argument('projectid',type=click.STRING,nargs=1)
@click.argument('sampleid',type=click.STRING,nargs=-1)
def sample(ctx, projectid, sampleid):
    """ Deliver the specified sample to the specified destination
    """
    for sid in sampleid:
        d = _deliver.SampleDeliverer(
            projectid,
            sid,
            **ctx.parent.params)
        _exec_delivery(d,d.deliver_sample)

# helper function to handle error reporting
def _exec_delivery(deliver_obj,deliver_fn):
    try:
        if deliver_fn():
            deliver_obj.log.info(
                "{} delivered successfully".format(str(deliver_obj)))
        else:
            deliver_obj.log.info(
                "{} delivered with some errors, check log".format(
                    str(deliver_obj)))
    except Exception as e:
        try:
            send_mail(
                subject="[ERROR] a delivery failed: {}".format(str(deliver_obj)),
                content="Project: {}\nSample: {}\nCommand: {}\n\n"\
                    "Additional information:{}\n".format(
                        deliver_obj.projectid,
                        deliver_obj.sampleid,
                        str(deliver_fn),
                        str(e)
                    ),
                recipient=d.config.get('operator'))
        except Exception as me:
            deliver_obj.log.error(
                "delivering {} failed - reason: {}, but operator {} could not "\
                "be notified - reason: {}".format(
                    str(deliver_obj),e,deliver_obj.config.get('operator'),me))
        else:
            d.log.error("delivering {} failed - reason: {}, operator {} has been "\
                "notified".format(
                    str(deliver_obj),str(e),deliver_obj.config.get('operator')))
