import os
import shutil
import subprocess
from taca.log import LOG
from taca.utils.filesystem import create_folder
from taca.utils.misc import hashfile, call_external_command

class TransferAgent(object):
    
    def __init__(
        self, 
        src_path=None, 
        dest_path=None,
        opts={},
        **kwargs):
        self.src_path = src_path
        self.dest_path = dest_path
        self.log = kwargs.get('log',LOG)
        self.cmdopts = opts

    def __str__(self):
        return type(self).__name__
                
    def do_transfer(self):
        raise NotImplementedError("This method should be implemented by "\
        "subclass")
    
    def validate_src_path(self):
        if self.src_path is None:
            raise TransferError(
                msg="src_path cannot be None",
                src_path=self.src_path,
                dest_path=self.dest_path)
        if not os.path.exists(self.src_path):
            raise TransferError(
                msg="src_path '{}' does not exist".format(self.src_path),
                src_path=self.src_path,
                dest_path=self.dest_path)
    
    def validate_dest_path(self):
        if self.dest_path is None:
            raise TransferError(
                msg="dest_path cannot be None",
                src_path=self.src_path,
                dest_path=self.dest_path)
                
    def validate_transfer(self):
        raise NotImplementedError("This method should be implemented by "\
        "subclass")
    
class RsyncAgent(TransferAgent):
    
    CMD = "rsync"
    DEFAULT_OPTS = {
        "-a": None,
    }
    
    def __init__(
        self, 
        src_path, 
        remote_host=None, 
        dest_path=None,
        remote_user=None, 
        validate=True,
        digestfile=None,
        opts=None, 
        **kwargs):
        super(RsyncAgent,self).__init__(
            src_path=src_path,
            dest_path=dest_path,
            opts=opts or self.DEFAULT_OPTS,
            **kwargs)
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.validate = validate
        self.digestfile = digestfile

    def do_transfer(self,transfer_log=None):
        self.validate_src_path()
        self.validate_dest_path()
        cmdopts = []
        for param,val in self.cmdopts.items():
            if val is None:
                cmdopts.append(param)
            else:
                if type(val) == str:
                    val = [val]
                for v in val:
                    cmdopts.append("{}={}".format(param,v))
        command = [self.CMD] + cmdopts + [self.src_path,self.remote_path()]
        try:
            call_external_command(
                command,with_log_files=True,prefix=transfer_log)
        except subprocess.CalledProcessError as e:
            raise RsyncError(e)
        return (not self.validate) or self.validate_transfer()
        
    def remote_path(self):
        return "{}{}{}".format(
            "{}@".format(self.remote_user) \
                if self.remote_user is not None \
                else "",
            "{}:".format(self.remote_host) \
                if self.remote_host is not None \
                else "",
            self.dest_path \
                if self.dest_path is not None \
                else "" 
        )
        
    def validate_dest_path(self):
        if self.dest_path is None and self.remote_host is None:
            raise TransferError(
                msg="dest_path and remote_host cannot both be None",
                src_path=self.src_path)
        if self.remote_user is not None and self.remote_host is None:
            raise TransferError(
                msg="dest_path cannot be None if remote_user is not None",
                src_path=self.src_path)
                
    def validate_transfer(self):
        if self.remote_host is not None:
            raise NotImplementedError("Validation on remote host not implemented")
        try:
            with open(self.digestfile) as fh:
                hasher = self.digestfile.split('.')[-1]
                dpath = os.path.dirname(self.digestfile)
                for line in fh:
                    digest,fpath = line.split()
                    if digest != hashfile(
                        os.path.join(dpath,fpath),
                        hasher=hasher):
                        return False
        except TypeError as e:
            raise RsyncValidationError(
                "no digest file specified",
                self.src_path,
                self.dest_path)
        return True

class SymlinkAgent(TransferAgent):
    
    def __init__(self, src_path, dest_path, overwrite=True, relative=True, **kwargs):
        super(SymlinkAgent,self).__init__(
            src_path=src_path,
            dest_path=dest_path,
            **kwargs)
        self.overwrite = overwrite
        self.relative = relative

    def do_transfer(self, validate=True):
        self.validate_src_path()
        self.validate_dest_path()
        try:
            if os.path.exists(self.dest_path):
                # If the existing target is a symlink that points to the 
                # source, we're all good
                if self.validate_transfer():
                    self.log.debug(
                        "target exists and points to the correct "\
                        "source path: '{}'".format(self.src_path))
                    return True
                # If we are not overwriting, return False
                if not self.overwrite:
                    self.log.debug(
                        "target '{}' exists and will not be "\
                        "overwritten".format(self.dest_path))
                    return False
                # If the target is a mount, let's not mess with it
                if os.path.ismount(self.dest_path):
                    raise SymlinkError("target exists and is a mount")
                # If the target is a link or a file, we remove it
                if os.path.islink(self.dest_path) or \
                    os.path.isfile(self.dest_path):
                    self.log.debug(
                        "removing existing target file '{}'".format(
                            self.dest_path))
                    os.unlink(self.dest_path)
                # If the target is a directory, we remove it and
                # everything underneath
                elif os.path.isdir(self.dest_path):
                    self.log.debug(
                        "removing existing target folder "\
                        "'{}'".format(self.dest_path))
                    shutil.rmtree(self.dest_path)
                # If it's something else, let's bail out
                else:
                    raise SymlinkError(
                        "target exists and will not be overwritten")
            if not create_folder(os.path.dirname(self.dest_path)):
                raise SymlinkError("failed to create target folder hierarchy")
            # If we should create a relative symlink, determine the relative path
            os.symlink(
                os.path.relpath(self.src_path,os.path.dirname(self.dest_path)) \
                if self.relative else \
                self.src_path,
                self.dest_path)
        except OSError as e:
            raise SymlinkError(e)
        if validate and not self.validate_transfer():
            raise SymlinkError("target was not created properly")
        return True
        
    def validate_transfer(self):
        return os.path.exists(self.dest_path) and \
            os.path.islink(self.dest_path) and \
            os.path.samefile(self.src_path,self.dest_path)

class TransferError(Exception):
    def __init__(self,msg,src_path=None,dest_path=None):
        super(TransferError,self).__init__(msg)
        self.src_path = src_path
        self.dest_path = dest_path

class SymlinkError(TransferError): pass
class RsyncError(TransferError): pass
class RsyncValidationError(TransferError): pass
    
   