#!/usr/bin/python

'''
This script uses btrfs send/receive to send snapshots to a remote host.

Assumptions:
    all snapshots are in the same directory, both on local and remote side
    all snapshots on the remote directory are copies from the local directory
    all snapshots in those directories are to be synchronized
    snapshot alphabetical order is the order in which they were created

This works well together with https://github.com/mmehnert/btrfs-snapshot-rotation .
snapshots on the remote side that don't exist on the local side (because
they were rotated) can be deleted with delete_unmatching_remote_snaps()
'''

LOCK_FILE= '/media/btrfs/btrfs_lockfile'                #A lock file to prevent multiple running processes, or running after critical error
LOCAL_SNAP_DIR= '/media/btrfs/home/test'    #The local directory where btrfs snapshots are kept
REMOTE_SNAP_DIR= '/media/btrfs/test/snapshots'    #The remote directory where btrfs snapshots are kept
REMOTE_HOST= "127.0.0.1"


import os
import sys
import subprocess
import socket
import logging
from fabric.api import env, settings, run
from fabric.state import default_channel

log= logging.getLogger(__name__)
log.addHandler( logging.StreamHandler(sys.stdout) )
log.setLevel(logging.INFO)




def rformat( string, kwargs ):
    '''formats a string multiple times as needed.'''
    f= string.format(**kwargs)
    return f if f==string else rformat(f, kwargs)

def remote_pipe(local_command, remote_command, buf_size=1024*1024):
    '''executes a local command and a remove command (with fabric), and
    sends the local's stdout to the remote's stdin'''
    local_p= subprocess.Popen(local_command, shell=True, stdout=subprocess.PIPE)
    channel= default_channel() #fabric function
    channel.set_combine_stderr(True)
    channel.settimeout(2)
    channel.exec_command( remote_command )
    try:
        read_bytes= local_p.stdout.read(buf_size)
        while read_bytes:
            channel.sendall(read_bytes)
            read_bytes= local_p.stdout.read(buf_size)
    except socket.error:
        local_p.kill()
        #fail to send data, let's see the return codes and received data...
    local_ret= local_p.wait()
    received= channel.recv(buf_size)
    channel.shutdown_write()
    channel.shutdown_read()
    remote_ret= channel.recv_exit_status()
    if local_ret!=0 or remote_ret!=0:
        raise Exception("remote_pipe failed. Local retcode: {0} Remote retcode: {1}  output: {2}".format(local_ret, remote_ret, received))

def send_snapshot_to_remote( local_snap_path, remote_snap_path, local_delta_snap_path=None, compress=False, progress=True):
    '''sends a snapshot to a remote subvolume'''
    lsp, rsp, ldsp= local_snap_path, remote_snap_path, local_delta_snap_path
    parent= "-p {ldsp}" if ldsp else ""
    ssc= "/sbin/btrfs send {parent} {lsp}" if ldsp else "/sbin/btrfs send {lsp}" #send stream command
    rsc= "/sbin/btrfs receive {rsp}" #receive stream command
    if progress:
        ssc+= " |pv"
    if compress:
        ssc+= " | gzip"
        rsc= "gzip -d | " + rsc
    ssc= rformat(ssc, locals())
    rsc= rformat(rsc, locals())
    log.debug("Executing local command: "+ssc)
    log.debug("Executing remote command: "+rsc)
    remote_pipe( ssc, rsc )


def snapshot_remote_subvolume( remote_subvol_path, remote_snap_path, writable=True):
    '''snapshots a remote subvolume'''
    ro= "" if writable else "-r "
    command= "/sbin/btrfs subvol snap {ro}{remote_subvol_path} {remote_snap_path}"
    run(command.format(**locals()))

def delete_remote_subvolume( remote_subvol_path ):
    run("/sbin/btrfs subvol del "+remote_subvol_path)

class SnapshotLists(object):
    '''represents a matching list of local and remote snapshots'''
    def __init__(self, local_dir, remote_dir):
        self.local_dir=  local_dir
        self.remote_dir= remote_dir
        self.refresh_local()
        self.refresh_remote()
        self.missing_remote= sorted(self.local_set-self.remote_set)
        log.debug("remote missing snapshots: "+"\n\t"+"\n\t".join(self.missing_remote))

    def _local_snapshots(self):
        snaps= sorted(os.listdir(LOCAL_SNAP_DIR))
        log.debug("local snapshots:  "+"\n\t"+"\n\t".join(snaps))
        return snaps

    def _remote_snapshots(self):
        command="ls -1 "+self.remote_dir
        snaps= run(command)
        snaps= snaps.split("\r\n") if "\r\n" in snaps else snaps.split("\n")
        snaps.sort()
        log.debug("remote snapshots: "+"\n\t"+"\n\t".join(snaps))
        return snaps

    def add_remote(self, snap):
        '''manually add a remote snapshot as existing'''
        self.remote.append( snap )
        self.remote.sort()
        self.remote_set|=set(snap)

    def refresh_local(self):
        self.local=  self._local_snapshots()
        self.local_set=  set(self.local)

    def refresh_remote(self):
        self.remote= self._remote_snapshots()
        self.remote_set= set(self.remote)

    def get_local_path( self, snap ):
        assert snap in self.local_set
        return os.path.join( self.local_dir, snap)

    def get_remote_path( self, snap ):
        return os.path.join( self.remote_dir, snap)

    def find_best_delta(self, snap):
        '''given a snapshot SNAP, finds the best DELTA snapshot to use
        to transfer SNAP it to the remote: the latest snapshot before
        SNAP that exists both locally and remotelly.'''
        assert snap in self.local and not snap is self.remote
        on_both= sorted(self.local_set & self.remote_set)
        before= filter(lambda x: x<snap, on_both)
        delta= before[-1] if before else None
        log.debug("Best delta for {0} is {1}".format(snap, delta))
        return delta


class BtrfsSnapshotSender( object ):
    def __init__(self, lock_file, local_snap_dir, remote_snap_dir):
        self.lock_file= lock_file
        self._open_lockfile()
        self.lists= SnapshotLists(local_snap_dir, remote_snap_dir)

    def __del__(self):
        self._close_lockfile()

    def _open_lockfile(self):
        if os.path.exists(self.lock_file):
            self._close_lockfile= lambda: None
            raise Exception("Previous transfer still running, or interrupted. Exiting")
        open(self.lock_file, 'w').close()

    def _close_lockfile(self):
        os.remove(self.lock_file)

    def _sync_snapshot( self, snap, delta=None ):
        log.info("syncing "+snap+(" with delta "+delta if delta else ""))
        local=  self.lists.get_local_path(  snap )
        remote= self.lists.remote_dir
        delta=  self.lists.get_local_path(  delta ) if delta else None
        send_snapshot_to_remote( local, remote, delta )
        self.lists.add_remote(snap)

    def delete_unmatching_remote_snaps(self):
        '''deletes snapshots on remote host that don't exist on local host'''
        log.warning("deleting unmatching snapshots: "+",".join(self.lists.missing_remote))
        for snap in unmatching:
            delete_remote_subvolume( map(lists.remote_path, self.lists.missing_remote) )

    def sync_one(self):
        '''Sends the next missing snapshot to the remote.
        Returns the name of the synced snapshot, or None if there's
        nothing missing'''
        try:
            snap= self.lists.missing_remote[0] #first missing snapshot
            delta= self.lists.find_best_delta( snap )
            self._sync_snapshot( snap, delta )
            return snap
        except IndexError:
            log.debug("nothing missing")

    def sync_all(self):
        '''Sends all the missing snapshots to the remote.
        Returns the names of the synced snapshots'''
        done=[]
        synced= self.sync_one()
        while synced:
            done.append(synced)
            synced= self.sync_one()

if __name__=="__main__":
    from fabric.state import output
    output.running=False            #suppress command printouts
    output.stdout=False             #show stdout of executed commands
    output.stderr=True              #same for stderr
    env.hosts=env.all_hosts= [REMOTE_HOST]
    with settings(host_string=REMOTE_HOST):
        sender= BtrfsSnapshotSender(LOCK_FILE, LOCAL_SNAP_DIR, REMOTE_SNAP_DIR)
        sender.sync_one() 
