""" CLI for the deliver subcommand
"""
import click
from ngi_pipeline.utils.communication import mail_analysis
from taca.deliver import deliver as _deliver

@click.group()
@click.option('--deliverypath', type=click.STRING,
			  help="Deliver to this destination folder")
@click.option('--stagingpath', type=click.STRING,
			  help="Stage the delivery under this path")
@click.option('--operator', type=click.STRING, default=None, multiple=True,
			  help="Email address to notify operator at. Multiple operators can be specified")
def deliver(deliverypath,stagingpath,operator):
    """ Deliver methods entry point
    """
    pass
    
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
        **{k:v for k,v in ctx.parent.params.items() \
            if v is not None and len(v) > 0})
    try:
        if d.deliver_project():
            d.log.info("All samples in {} successfully delivered".format(
                str(d)))
        else:
            d.log.info("Some samples in {} failed to be delivered "\
                "properly".format(str(d)))
    except Exception as e:
        try:
            mail_analysis(
                projectid,
                engine_name="Project Delivery",
                info_text=str(e),
                recipient=d.config.get('operator'),
                subject="project delivery failed: {}".format(str(d)),
                origin="taca deliver project"
            )
        except Exception as me:
            d.log.error("delivering {} failed - reason: {}, but operator {} could not "\
                "be notified - reason: {}".format(
                str(d),e,d.config.get('operator'),me))
        else:
            d.log.error("delivering {} failed - reason: {}, operator {} has been "\
                "notified".format(str(d),str(e),d.config.get('operator')))

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
            **{k:v for k,v in ctx.parent.params.items() if v is not None and len(v) > 0})
        try:
            d.deliver_sample()
        except Exception as e:
            try:
                mail_analysis(
                    projectid,
                    sample_name=sid,
                    engine_name="Sample Delivery",
                    info_text=str(e),
                    recipient=d.config.get('operator'),
                    subject="a sample delivery failed: {}".format(str(d)),
                    origin="taca deliver sample"
                )
            except Exception as me:
                d.log.error("delivering {} failed - reason: {}, but operator {} could not "\
                    "be notified - reason: {}".format(
                    str(d),e,d.config.get('operator'),me))
            else:
                d.log.error("delivering {} failed - reason: {}, operator {} has been "\
                    "notified".format(str(d),str(e),d.config.get('operator')))