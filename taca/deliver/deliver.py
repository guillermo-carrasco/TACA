"""Deliver methods for projects and samples"""
import datetime
import glob
import hashlib
import os
import re
from ngi_pipeline.database import classes as db
from ngi_pipeline.utils.classes import memoized
from taca.log import get_logger
from taca.utils.config import get_config
from taca.utils.filesystem import create_folder
from taca.utils.misc import hashfile
from taca.utils import transfer

class DelivererError(Exception): pass
class DelivererDatabaseError(DelivererError): pass
class DelivererReplaceError(DelivererError): pass
class DelivererRsyncError(DelivererError): pass

class Deliverer(object):
    
    def __init__(self, projectid, sampleid, **kwargs):
        self.config = get_config().get('deliver',{})
        self.config.update(kwargs)
        self.log = get_logger()
        self.hasher = kwargs.get('hash_algorithm','sha1')
        for k,v in self.config.items():
            setattr(self,k,v)
        self.projectid = projectid
        self.sampleid = sampleid
        self.projectalias = getattr(
            self,'projectalias',self.fetch_projectalias())

    def __str__(self):
        return "{}:{}".format(
            self.projectid,self.sampleid) \
            if self.sampleid is not None else self.projectid

    def fetch_projectalias(self):
        try:
            return self.project_entry(self.projectid)['projectid']
        except KeyError:
            raise DelivererDatabaseError(
                "the project alias for project '{}' could not be fetched "\
                "from database".format(self.projectid))

    @memoized
    def dbcon(self):
        return db.CharonSession()

    @memoized
    def project_entry(self, projectid):
        return self.wrap_database_query(
            self.dbcon().project_get,projectid)

    @memoized
    def sample_entry(self, projectid, sampleid):
        return self.wrap_database_query(
            self.dbcon().sample_get,projectid,sampleid)

    def update_sample_delivery(self):
        return self.wrap_database_query(
            self.dbcon().sample_update,
            self.projectid,
            self.sampleid,
            delivery_status="DELIVERED")
            
    def wrap_database_query(self,query_fn,*query_args,**query_kwargs):
        try:
            return query_fn(*query_args,**query_kwargs)
        except db.CharonError as ce:
            raise DelivererDatabaseError(ce.message)
        except Exception:
            return {'projectid': 'hepp'}
            
    def gather_files(self):
        """ Generator function for locating files in the analysis folder that 
            should be included in the delivery and constructing the target 
            paths.
        
            :returns: A generator returning tuples with source file name, 
            destination file name and the sha1 hash
        """
        for sfile, dfile in getattr(self,'files_to_deliver',[]):
            for f in glob.iglob(self.expand_path(sfile)):
                if (os.path.isdir(f)):
                    for pdir,_,files in os.walk(f):
                        for current in files:
                            fpath = os.path.join(pdir,current)
                            fname = os.path.relpath(fpath,f)
                            yield(fpath,
                                os.path.join(self.expand_path(dfile),fname),
                                hashfile(fpath,hasher=self.hasher))
                else:
                    yield (f, 
                        os.path.join(self.expand_path(dfile),os.path.basename(f)), 
                        hashfile(f,hasher=self.hasher))
    
    def stage_delivery(self):
        """ Stage a delivery by symlinking files discovered by the gather_files
            function to the staging folder path specified in the configuration
            and the subfolders specified in the files_to_deliver list of tuples.
        """
        digestpath = self.staging_digestfile()
        create_folder(os.path.dirname(digestpath))
        try: 
            with open(digestpath,'w') as dh:
                agent = transfer.SymlinkAgent(None,None,relative=True,log=self.log)
                for src, dst, digest in self.gather_files():
                    agent.src_path = src
                    agent.dest_path = dst
                    try:
                        agent.do_transfer()
                        if digest is not None:
                            dh.write("{}  {}\n".format(
                                digest,
                                os.path.relpath(
                                    dst,self.expand_path(self.stagingpath))))
                    except transfer.TransferError as e:
                        self.log.warning("failed to stage file '{}' when "\
                            "delivering {} - reason: {}".format(
                                src,str(self),e))
                    except transfer.SymlinkError as e:
                        self.log.warning("failed to stage file '{}' when "\
                            "delivering {} - reason: {}".format(
                                src,str(self),e))
        except IOError as e:
            raise DelivererError(
                "failed to stage delivery - reason: {}".format(e))
        return True

    def delivered_digestfile(self):
        return self.expand_path(
            os.path.join(
                self.deliverypath,
                self.projectid,
                os.path.basename(self.staging_digestfile())))

    def staging_digestfile(self):
        return self.expand_path(
            os.path.join(
                self.stagingpath,
                "{}.{}".format(self.sampleid,self.hasher)))

    def transfer_log(self):
        return self.expand_path(
            os.path.join(
                self.stagingpath,
                "{}_{}.rsync".format(
                    self.sampleid,
                    datetime.datetime.now().strftime("%Y%m%dT%H%M%S"))))
                
    @memoized
    def expand_path(self,path):
        try:
            m = re.search(r'(_[A-Z]+_)',path)
        except TypeError:
            return path
        else:
            if m is None:
                return path
            try:
                expr = m.group(0)
                return self.expand_path(
                    path.replace(expr,getattr(self,expr[1:-1].lower())))
            except AttributeError as e:
                raise DelivererError(
                    "the path '{}' could not be expanded - reason: {}".format(
                        path,e))
    
class ProjectDeliverer(Deliverer):
    
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(ProjectDeliverer,self).__init__(
            projectid,
            sampleid,
            **kwargs)
            
    def deliver_project(self, destination_folder=None):
        """ Deliver a project to the destination
        """
        self.log.info(
            "Delivering {} to {}".format(str(self),destination_folder))
        return True

    def redeliver(self):
        return False

class SampleDeliverer(ProjectDeliverer):
    
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(SampleDeliverer,self).__init__(
            projectid,
            sampleid,
            **kwargs)
        
    def deliver_sample(self):
        """ Deliver a sample to the destination
        """
        self.log.info(
            "Delivering {} to {}".format(str(self),self.deliverypath))
        try:
            sampleentry = self.sample_entry(self.projectid,self.sampleid)
        except DelivererDatabaseError as e:
            self.log.error(
                "error '{}' occurred during delivery of {}".format(
                    str(e),str(self)))
            raise
        if sampleentry.get('delivery_status') == 'DELIVERED':
            if self.redeliver():
                self.log.error(
                    "{}:{} has previously been delivered and this delivery "\
                    "will not be overwritten".format(str(self)))
                raise DelivererReplaceError(
                    "a previous delivery has been made and should be replaced")
            self.log.info(
                "{}:{} has already been delivered".format(str(self)))
            return True
        elif not sampleentry.get('analysis_status') == 'ANALYZED':
            self.log.info("{} has not finished analysis and will not be "\
                "delivered".format(str(self)))
            return False
        else:
            # Propagate raised errors upwards, they should trigger 
            # notification to operator
            if not self.do_delivery():
                raise DelivererError("sample was not properly delivered")
            self.log.info("{} successfully delivered".format(str(self)))
            return True
    
    def do_delivery(self):
        self.stage_delivery()
        agent = transfer.RsyncAgent(
            self.expand_path(self.stagingpath),
            dest_path=self.expand_path(self.deliverypath),
            digestfile=self.delivered_digestfile(),
            remote_host=getattr(self,'remote_host',None), 
            remote_user=getattr(self,'remote_user',None), 
            log=self.log,
            opts={
                '--copy-links': None,
                '--recursive': None,
                '--perms': None,
                '--chmod': 'o+rwX,ug-rwx',
                '--verbose': None,
                '--exclude': "*.{}".format(self.transfer_log().split('.')[-1])
            })
        with open(self.transfer_log(),'w') as lh:
            try:
                return agent.do_transfer(transfer_log=lh)
            except transfer.TransferError as e:
                raise DelivererRsyncError(e)
    
            