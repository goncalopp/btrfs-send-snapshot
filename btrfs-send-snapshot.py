#!/usr/bin/python

'''
This script uses btrfs send/receive to send snapshots to a remote host.
It does this by:
    Creating a new subvolume on the remote host
    sending the btrfs send stream to the remote subvolume
    snapshotting the subvolume in the remote host
Assumptions:
    all snapshots are in the same directory, both on local and remote side
    all snapshots in those directories are to be synchronized
    snapshot alphabetical order is the order in which they were created
    
This works well together with https://github.com/mmehnert/btrfs-snapshot-rotation.
snapshots on the remote side that don't exist on the local side (because
they were rotated) can be deleted with delete_unmatching_remote_snaps()
'''

LOCK_FILE= '/home/test/btrfs_lockfile'                #A lock file to prevent multiple running processes, or running after critical error
LOCAL_SNAP_DIR= '/home/test/tmp1/snapshots'    #The local directory where btrfs snapshots are kept 
REMOTE_SNAP_DIR= '/home/test/tmp2/snapshots'    #The remote directory where btrfs snapshots are kept
REMOTE_SUBVOLUME_PATH= '/home/test/tmp2/rootfs' #The remote path of the btrfs subvolume where the snapshot will be written


import os
import subprocess
import logging
from fabric.api import *
from fabric.contrib import files
from fabric.state import default_channel

logging.basicConfig(level=logging.DEBUG)

class cached(object):    
    '''Computes attribute value and caches it in the instance.
    Python Cookbook (Denis Otkidach) http://stackoverflow.com/users/168352/denis-otkidach
    '''
    def __init__(self, method, name=None):
        self.method = method
        self.name = name or method.__name__
        self.__doc__ = method.__doc__
    def __get__(self, inst, cls):  
        if inst is None:
            return self
        result = self.method(inst)
        setattr(inst, self.name, result)
        return result

def rformat( string, kwargs ):
    '''formats a string multiple times as needed.'''
    f= string.format(**kwargs)
    return f if f==string else rformat(f, kwargs)

def remote_pipe(local_command, remote_command, buf_size=1024):
    '''executes a local command and a remove command (with fabric), and 
    sends the local's stdout to the remote's stdin'''
    local_p= subprocess.Popen(local_command, shell=True, stdout=subprocess.PIPE)
    channel= default_channel() #fabric function
    channel.exec_command( remote_command )
    read_bytes= local_p.stdout.read(buf_size)
    while read_bytes:
        channel.send(read_bytes)
    local_ret= local_p.wait()
    channel.shutdown_write()
    remote_ret= recv_exit_status(self)
    if local_ret!=0 or remote_ret!=0:
        raise Exception("remote_pipe failed")

def send_snapshot_to_remote_subvol( local_snap_path, remote_subvol_path, local_delta_snap_path=None, compress=True):
    '''sends a snapshot to a remote subvolume'''
    lsp, rsp, ldsp= local_snap_path, remote_subvol_path, local_delta_snap_path
    parent= "-p {ldsp}" if ldsp else ""
    ssc= "/sbin/btrfs send {parent} {lsp}" if ldsp else "btrfs send {lsp}" #send stream command
    rsc= "/sbin/btrfs receive {rsp}" #receive stream command
    if compress:
            ssc+= " | gzip"
            rsc= "gzip -d | " + rsc
    ssc= rformat(ssc, locals())
    rsc= rformat(rsc, locals())
    remote_pipe( ssc, rsc )
    

def snapshot_remote_subvolume( remote_subvol_path, remote_snap_path, writable=True):
    '''snapshots a remote subvolume'''
    ro= "" if writable else "-r "
    command= "/sbin/btrfs subvol snap {ro}{remote_subvol_path} {remote_snap_path}"
    run(command.format(**locals()))

def delete_remote_subvolume( remote_subvol_path ):
    run("sbin/btrfs subvol del "+remote_subvol_path)

def prepare_remote_subvol(remote_subvol_path, remote_snap_dir, base=None):
    '''If base is None, creates a empty subvolume.
    If base is given, a snapshot of base is created instead.'''
    if files.exists(remote_subvol_path):
        delete_subvolume( remote_subvol_path )
    if base:
        snapshot_subvolume(remote_snap_dir, remote_subvol_path, base )
    else:
        run("/sbin/btrfs subvol create"+remote_subvol_path)


class BtrfsSnapshotSender( object ):
    def __init__(self, lock_file, local_snap_dir, remote_snap_dir, remote_subvol_path):
        self.lock_file, self.local_snap_dir, self.remote_snap_dir, self.remote_subvol_path= lock_file, local_snap_dir, remote_snap_dir, remote_subvol_path
        self._open_lockfile()
    
    def __del__(self):
        self._close_lockfile()

    def _open_lockfile(self):
        if os.path.exists(self.lock_file):
            raise Exception("Previous transfer still running, or interrupted. Exiting")
        open(self.lock_file, 'w').close()

    def _close_lockfile(self):
        os.remove(self.lock_file)

    @cached
    def _local_snapshots(self):
        snaps= sorted(os.listdir(LOCAL_SNAP_DIR))
        logging.debug("local snapshots: "+",".join(snaps))
        return snaps
    
    @cached
    def _remote_snapshots(self):
        ssh= "ssh {self.remote_host}"
        command="{ssh} ls"
        snaps= sorted(execute_command(rformat(command)))
        logging.debug("remote snapshots: "+",".join(snaps))
        return snaps
    
    def _local_remote_sets(self):
        return set(self._local_snapshots),set(self._local_snapshots)
    
    def _missing_snapshots(self):
        '''snapshots that only exist on the local side'''
        local, remote= self._local_remote_sets()
        missing= sorted(local-remote)
        logging.debug("missing snapshots: "+",".join(missing))
        return missing
    
    def _sync_snapshot( self, snap, delta=None ):
        logging.debug("syncing "+snap+(" with delta "+delta if delta else ""))
        local_snap= os.path.join( self.local_snap_dir, snap)
        remote_snap= os.path.join( self.remote_snap_dir, snap)
        delta_snap= os.path.join(self.local_snap_dir, delta) if delta else None
        send_snapshot_to_remote_subvol( local_snap, self.remote_subvol_path, delta_snap )
        snapshot_remote_subvolume( self.remote_subvol_path, remote_snap)
        _remote_snapshots.append(snap)
        
    def _sync_snapshots(self, snapshots, delta=None):
        prepare_remote_subvol(self.remote_subvol_path, remote_snap_dir, delta)
        for snap in snapshots:
            self._sync_snapshot(snap, delta)
            delta=snap #delta of next snapshot is the current one
    
    def delete_unmatching_remote_snaps(self):
        '''deletes snapshots on remote host that don't exist on local host'''
        local, remote= self._local_remote_sets()
        unmatching= remote-local
        logging.warning("deleting unmatching snapshots: "+",".join(unmatching))
        for snap in unmatching:
            delete_remote_subvolume( os.path.join( self.remote_snap_dir, snap) )
    
    def sync_one(self):
        '''Sends the next missing snapshot to the remote. 
        Returns the name of the synced snapshot, or None if there's 
        nothing missing'''
        missing= self._missing_snapshots()
        if not missing:
            logging.debug("nothing missing")
            return None
        snap= missing[0] #first missing snapshot
        self._sync_snapshot( snap )
    
    def sync_all(self):
        '''Sends all the missing snapshots to the remote. 
        Returns the names of the synced snapshots'''
        missing= self._missing_snapshots()
        if not missing:
            logging.debug("nothing missing")
            return None
        self._sync_snapshots( missing )

sender= BtrfsSnapshotSender(LOCK_FILE, LOCAL_SNAP_DIR, REMOTE_SNAP_DIR, REMOTE_SUBVOLUME_PATH)
sender.sync_one()
