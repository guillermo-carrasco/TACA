""" Unit tests for the utils helper functions """

import hashlib
import mock
import os
import shutil
import subprocess
import tempfile
import unittest
from taca.utils import misc, filesystem, transfer

class TestMisc():  
    """ Test class for the misc functions """

    @classmethod
    def setUpClass(self):
        self.rootdir = tempfile.mkdtemp(prefix="test_taca_misc")
        self.hashfile = os.path.join(self.rootdir,'test_hashfile')
        with open(self.hashfile,'w') as fh:
            fh.write("This is some contents\n")
        self.hashfile_digests = {
            'SHA256':
                '4f075ae76b480bb0200dab01cd304f4045e04cd2b73e88b89549e5ac1627f222',
            'MD5':
                'c8498fc299bc3e22690045f1b62ce4e9',
            'SHA1':
                '098fb272dfdae2ea1ba57c795dd325fa70e3c3fb'}
            
    @classmethod        
    def tearDownClass(self):
        shutil.rmtree(self.rootdir)
    
    # Test generator for different hashing algorithms
    def test_hashfile(self):
        for alg,obj in self.hashfile_digests.items():
            yield self.check_hash, alg, obj
    
    def test_hashfile_dir(self):
        """Hash digest for a directory should be None"""   
        assert misc.hashfile(self.rootdir) is None
        
    def test_hashfile_none(self):
        """Hash digest for algorithm None should be None"""   
        assert misc.hashfile(self.hashfile,hasher=None) is None
    
    def test_multiple_hashfile_calls(self):
        """ Ensure that the hasher object is cleared between subsequent calls
        """
        assert misc.hashfile(self.hashfile,hasher='sha1') == misc.hashfile(self.hashfile,'sha1')
        
    def check_hash(self, alg, exp):
        assert misc.hashfile(self.hashfile,hasher=alg) == exp
        

class TestFilesystem(unittest.TestCase):
    """ Test class for the filesystem functions """

    def setUp(self):
        self.rootdir = tempfile.mkdtemp(prefix="test_taca_filesystem")
        
    def tearDown(self):
        shutil.rmtree(self.rootdir)
    
    def test_crete_folder1(self):
        """ Ensure that a non-existing folder is created """
        target_folder = os.path.join(self.rootdir,"target-non-existing")
        self.assertTrue(
            filesystem.create_folder(target_folder),
            "A non-existing target folder could not be created")
        self.assertTrue(
            os.path.exists(target_folder),
            "A non-existing target folder was not created \
            but method returned True"
        )
    
    def test_crete_folder2(self):
        """ Ensure that an existing folder is detected """
        self.assertTrue(
            filesystem.create_folder(self.rootdir),
            "A pre-existing target folder was not detected")
    
    def test_crete_folder3(self):
        """ Ensure that a non-existing parent folder is created """
        target_folder = os.path.join(
            self.rootdir,
            "parent-non-existing",
            "target-non-existing")
        self.assertTrue(
            filesystem.create_folder(target_folder),
            "A non-existing parent and target folder could not be created")
        self.assertTrue(
            os.path.exists(target_folder),
            "A non-existing parent folder was not created \
            but method returned True"
        )
    
    def test_crete_folder3(self):
        """ Ensure that create_folder handles thrown exceptions gracefully """
        with mock.patch.object(filesystem.os,'makedirs',side_effect=OSError):
            self.assertFalse(
                filesystem.create_folder(
                    os.path.join(self.rootdir,"target-non-existing")),
                "A raised exception was not handled properly")

class TestTransferAgent(unittest.TestCase):
    """ Test class for the TransferAgent class """

    @classmethod
    def setUpClass(self):
        self.rootdir = tempfile.mkdtemp(prefix="test_taca_transfer_src")
        self.testfile = tempfile.mkstemp(dir=self.rootdir)
    
    @classmethod        
    def tearDownClass(self):
        shutil.rmtree(self.rootdir)
    
    def setUp(self):
        self.destdir = tempfile.mkdtemp(prefix="test_taca_transfer_dest")
        self.agent = transfer.TransferAgent(
            src_path=self.rootdir,
            dest_path=self.destdir)
            
    def tearDown(self):
        shutil.rmtree(self.destdir)
        
    def test_transfer_validate_src_path(self):
        """ src_path should validate properly """
        self.agent.validate_src_path()
        self.agent.src_path = None
        with self.assertRaises(transfer.TransferError):
            self.agent.validate_src_path()
        self.agent.src_path = os.path.join(
            self.rootdir,
            "this-file-does-not-exist")
        with self.assertRaises(transfer.TransferError):
            self.agent.validate_src_path()
                    
    def test_transfer_validate_dest_path(self):
        """ dest_path should validate properly """
        self.agent.validate_dest_path()
        self.agent.dest_path = None
        with self.assertRaises(transfer.TransferError):
            self.agent.validate_dest_path()

    def test_transfer_do_transfer(self):
        """ do_transfer in superclass should raise exception if called """
        with self.assertRaises(NotImplementedError):
            self.agent.do_transfer()


    def test_transfer_validate_transfer(self):
        """ validate_transfer in superclass should raise exception if called """
        with self.assertRaises(NotImplementedError):
            self.agent.validate_transfer()

class TestSymlinkAgent(unittest.TestCase):
    """ Test class for the SymlinkAgent class """

    @classmethod
    def setUpClass(self):
        self.rootdir = tempfile.mkdtemp(prefix="test_taca_symlink_src")
        path = self.rootdir
        for n in xrange(3):
            open(os.path.join(path,"file{}".format(n)),'w').close()
            path = os.path.join(path,"folder{}".format(n))
            os.mkdir(path)
    
    @classmethod        
    def tearDownClass(self): 
        shutil.rmtree(self.rootdir)
        
    def setUp(self):
        self.targetdir = tempfile.mkdtemp(
            prefix="test_taca_filesystem_symlink_dest")
        
    def tearDown(self):
        shutil.rmtree(self.targetdir)
    
    def test_symlink_validate_transfer(self):
        src = os.path.join(self.rootdir,"file0")
        dst = os.path.join(self.targetdir,"file0")
        os.symlink(src,dst)
        self.assertTrue(transfer.SymlinkAgent(src,dst).validate_transfer())

    def test_symlink_file1(self):
        """ Symlink a single file in the top folder """
        src = os.path.join(self.rootdir,"file0")    
        target = os.path.join(self.targetdir,os.path.basename(src))
        self.assertTrue(transfer.SymlinkAgent(src,target).do_transfer())
        
    def test_symlink_file2(self):    
        """ Symlnik a single file into a non-existing folder """
        src = os.path.join(self.rootdir,"folder0","folder1","file2")
        target = os.path.join(
            self.targetdir,
            "these","folders","should","be","created")
        self.assertTrue(transfer.SymlinkAgent(src,target).do_transfer())
 
    def test_symlink_file3(self):
        """ Replace an existing file with overwrite """
        src = os.path.join(self.rootdir,"file0")    
        target = os.path.join(self.targetdir,os.path.basename(src))
        open(target,'w').close()
        self.assertTrue(transfer.SymlinkAgent(src,target).do_transfer())
        
    def test_symlink_file4(self):
        """ Don't replace an existing file without overwrite """
        src = os.path.join(self.rootdir,"file0")    
        target = os.path.join(self.targetdir,os.path.basename(src))
        open(target,'w').close()
        self.assertFalse(
            transfer.SymlinkAgent(src,target,overwrite=False).do_transfer())
        
    def test_symlink_file5(self):
        """ Don't create a broken symlink """
        src = os.path.join(self.rootdir,"non-existing-file")    
        target = os.path.join(self.targetdir,os.path.basename(src))
        with self.assertRaises(transfer.TransferError):
            transfer.SymlinkAgent(src,target).do_transfer()
    
    def test_symlink_file6(self):
        """ Failing to remove existing file should raise SymlinkError 
        """
        src = self.rootdir
        target = os.path.join(self.targetdir,"target-file")
        open(target,'w').close()
        with mock.patch.object(
            transfer.os,
            'unlink',
            side_effect=OSError("Mocked error")):
            with self.assertRaises(transfer.SymlinkError):
                transfer.SymlinkAgent(src,target).do_transfer()
            
    def test_symlink_file7(self):
        """ Failing to validate created symlink should raise SymlinkError 
        """
        src = self.rootdir
        target = os.path.join(self.targetdir,os.path.basename(src))
        with mock.patch.object(
            transfer.SymlinkAgent,
            'validate_transfer',
            return_value=False):
            with self.assertRaises(transfer.SymlinkError):
                transfer.SymlinkAgent(src,target).do_transfer()
        
    def test_symlink_folder1(self):
        """ Symlinking a top-level folder """
        src = os.path.join(self.rootdir,"folder0")
        target = os.path.join(self.targetdir,os.path.basename(src))
        self.assertTrue(transfer.SymlinkAgent(src,target).do_transfer())
        
    def test_symlink_folder2(self):
        """ Replace an existing folder with overwrite """
        src = os.path.join(self.rootdir,"folder0")
        target = os.path.join(self.targetdir,os.path.basename(src))
        shutil.copytree(src,target)
        self.assertTrue(transfer.SymlinkAgent(src,target).do_transfer())
        
    def test_symlink_folder3(self):
        """ Don't overwrite a mount point """
        src = os.path.join(self.rootdir)    
        target = os.path.join(self.targetdir)
        with mock.patch.object(transfer.os.path,'ismount',return_value=True):
            with self.assertRaises(transfer.SymlinkError):
                transfer.SymlinkAgent(src,target).do_transfer()
            
    def test_symlink_folder4(self):
        """ Don't overwrite an existing path that is neither a mount point,
            file, link or directory
        """
        src = os.path.join(self.rootdir)    
        target = os.path.join(self.targetdir)
        with mock.patch('taca.utils.transfer.os.path') as mockobj:
            mockobj.ismount.return_value = False
            mockobj.isfile.return_value = False
            mockobj.islink.return_value = False
            mockobj.isdir.return_value = False
            with self.assertRaises(transfer.SymlinkError):
                transfer.SymlinkAgent(src,target).do_transfer()
    
    def test_symlink_folder5(self):
        """ Failing to create parent folder structure should raise SymlinkError 
        """
        src = self.rootdir
        target = os.path.join(self.targetdir,"non-existing-folder","target-file")
        with mock.patch.object(transfer,'create_folder',return_value=False):
            with self.assertRaises(transfer.SymlinkError):
                transfer.SymlinkAgent(src,target).do_transfer()
    
    def test_symlink_folder6(self):
        """ Failing to remove existing folder should raise SymlinkError 
        """
        src = self.rootdir
        target = self.targetdir
        with mock.patch.object(
            transfer.shutil,
            'rmtree',
            side_effect=OSError("Mocked error")):
            with self.assertRaises(transfer.SymlinkError):
                transfer.SymlinkAgent(src,target).do_transfer()
        
    def test_symlink_folder7(self):
        """ Failing to create symlink should raise SymlinkError 
        """
        src = self.rootdir
        target = os.path.join(self.targetdir,os.path.basename(src))
        with mock.patch.object(
            transfer.os,
            'symlink',
            side_effect=OSError("Mocked error")):
            with self.assertRaises(transfer.SymlinkError):
                transfer.SymlinkAgent(src,target).do_transfer()
                
    def test_symlink_folder8(self):
        """ An unexpected exception should propagate upwards 
        """
        src = self.rootdir
        target = self.targetdir
        with mock.patch.object(
            transfer.os.path,
            'exists',
            side_effect=Exception("Mocked error")):
            with self.assertRaises(Exception):
                transfer.SymlinkAgent(src,target).do_transfer()
    
class TestRsyncAgent(unittest.TestCase):
    """ Test class for the RsyncAgent class """

    @classmethod
    def setUpClass(self):
        self.rootdir = tempfile.mkdtemp(prefix="test_taca_transfer_src")
        (fh, self.testfile) = tempfile.mkstemp(
            prefix="test_taca_transfer_file")
        os.write(fh,"this is some content")
        os.close(fh)
        open(os.path.join(self.rootdir,"file0"),'w').close()
        f = os.path.join(self.rootdir,"folder0")
        os.mkdir(f)
        open(os.path.join(f,"file1"),'w').close()
        
    @classmethod        
    def tearDownClass(self):
        shutil.rmtree(self.rootdir)
        os.unlink(self.testfile)
        
    def setUp(self):
        self.destdir = tempfile.mkdtemp(prefix="test_taca_transfer_dest")
        self.agent = transfer.RsyncAgent(
            self.rootdir,
            dest_path=self.destdir,
            validate=False)
        
    def tearDown(self):
        shutil.rmtree(self.destdir)
              
    def test_rsync_validate_transfer(self):
        """ validate_transfer 
        """
        self.agent.remote_host = "not None"
        with self.assertRaises(NotImplementedError):
            self.agent.validate_transfer()
        self.agent.remote_host = None
        with self.assertRaises(transfer.RsyncValidationError):
            self.agent.validate_transfer()
                      
    def test_rsync_validate_dest_path(self):
        """ Destination path should be properly checked
        """
        try:
            self.agent.validate_dest_path()
        except transfer.TransferError as e:
            self.fail("a proper path raised an exception: {}".format(e))
        self.agent.remote_host = None
        self.agent.dest_path = None
        with self.assertRaises(transfer.TransferError):
            self.agent.validate_dest_path()
        self.agent.remote_user = "user"
        self.agent.dest_path = self.destdir
        with self.assertRaises(transfer.TransferError):
            self.agent.validate_dest_path()
         
    def test_rsync_agent1(self):
        """ Destination path should be properly constructed """
        self.assertEqual(
            self.destdir,
            self.agent.remote_path(),
            "Destination path was not correct for empty remote user " \
            "and empty destination host")
        self.agent.remote_host = "localhost"
        self.assertEqual(
            "localhost:{}".format(self.destdir),
            self.agent.remote_path(),
            "Destination path was not correct for empty remote user")
        self.agent.remote_user = "user"
        self.assertEqual(
            "user@localhost:{}".format(self.destdir),
            self.agent.remote_path(),
            "Destination path was not correct for non-empty remote user")
        self.agent.dest_path = None
        self.assertEqual(
            "user@localhost:",
            self.agent.remote_path(),
            "Destination path was not correct for empty destination path")
    
    def test_rsync_agent2(self):
        """ An error thrown by the rsync subprocess should be wrapped and 
            propagated 
        """
        with mock.patch.object(
            transfer.subprocess,'check_call',
                side_effect=subprocess.CalledProcessError(
                    cmd="mocked subprocess",
                    returncode=-1)):
            with self.assertRaises(transfer.RsyncError):
                self.agent.do_transfer()
                
    def test_rsync_agent3(self):
        """ rsync transfer of a single file """
        self.agent.src_path = os.path.join(self.rootdir,"file0")
        self.assertTrue(
            self.agent.do_transfer(),
            "transfer a single file failed")
        self.assertTrue(
            self.validate_files(
                self.agent.src_path,
                os.path.join(
                    self.destdir,
                    os.path.basename(self.agent.src_path))),
            "test file was not properly transferred")
                
    def test_rsync_agent4(self):
        """ rsync transfer of a folder """
        self.agent.src_path = os.path.join(self.rootdir,"folder0")
        self.assertTrue(
            self.agent.do_transfer(),
            "transfer a folder failed")
        self.assertTrue(
            self.validate_folders(
                self.agent.src_path,
                os.path.join(
                    self.destdir,
                    os.path.basename(self.agent.src_path))),
            "folder was not properly transferred")
                
    def test_rsync_agent5(self):
        """ rsync should be able to resolve symlinks """
        self.agent.src_path = os.path.join(self.rootdir,"folder0")
        os.symlink(self.testfile,os.path.join(self.agent.src_path,"link1"))
        self.agent.cmdopts = {'-a': None, '--copy-links': None}
        self.assertTrue(
            self.agent.do_transfer(),
            "transfer a folder containing a symlink failed")
        self.assertEqual(
            misc.hashfile(self.testfile,hasher='sha1'),
            misc.hashfile(
                os.path.join(self.destdir,"folder0","link1"),
                hasher='sha1'),
            "symlink was not properly transferred")

    def validate_folders(self,src,dst):
        for root, dirs, files in os.walk(src):
            for file in files:
                s = os.path.join(root,file)
                d = os.path.join(dst,os.path.relpath(s,src))
                if not self.validate_files(s,d):
                    return False
        return True
    
    def validate_files(self,src,dst):
        return os.path.exists(src) and \
            os.path.isfile(src) and \
            os.path.exists(dst) and \
            os.path.isfile(dst) and \
            misc.hashfile(src) == misc.hashfile(dst)