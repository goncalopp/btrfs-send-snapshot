#!/usr/bin/python

'''
This script uses btrfs send/receive to send snapshots to a remote host.
It assumes all snapshots are in the same directory, and their alphabetical order is
the order in which they were created.
This works well together with https://github.com/mmehnert/btrfs-snapshot-rotation .
'''

LAST_SYNC_FILE= '/home/goncalopp/btrfs_last_sync'          #A file that contains the name of the last successfuly sinced snapshot
LOCK_FILE= '/home/goncalopp/btrfs_lockfile'                #A lock file to prevent multiple running processes, or running after critical error
LOCAL_SNAP_DIR= '/home/goncalopp/tmp1/snapshots'    #The local directory where btrfs snapshots are kept 
REMOTE_HOST='localhost'                             #the hostname or IP of the remote host
REMOTE_SNAP_DIR= '/home/goncalopp/tmp2/snapshots'    #The remote directory where btrfs snapshots are kept
REMOTE_SUBVOLUME_PATH= '/home/goncalopp/tmp2/rootfs' #The remote path of the btrfs subvolume where the snapshot will be written

import os
import subprocess
import logging
logging.basicConfig(level=logging.DEBUG)

def rformat( string, kwargs ):
    '''formats a string multiple times as needed.'''
    f= string.format(**kwargs)
    return f if f==string else rformat(f, kwargs)
    
def execute_command(command, check=True):
    logging.debug("executing "+command)
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    separator="-----------------------------------------------------------\n"
    logging.debug("command output: "+str(out)+"--\n"+str(err))
    if check and p.returncode!=0:
        logging.warn("command output: \n"+separator+str(out)+separator+str(err)+separator)
        raise Exception("Process returned "+str(p.returncode))

def send_snapshot_to_subvol( local_snap_path, remote_host, remote_subvol_path, local_delta_snap_path=None, compress=True):
    '''sends a snapshot to a subvolume'''
    lsp, rsp, ldsp= local_snap_path, remote_subvol_path, local_delta_snap_path
    ssh= "ssh {remote_host}"
    parent= "-p {ldsp}" if ldsp else ""
    ssc= "/sbin/btrfs send {parent} {lsp}" if ldsp else "btrfs send {lsp}" #send stream command
    rsc= "/sbin/btrfs receive {rsp}" #receive stream command
    if compress:
            ssc+= " | gzip"
            rsc= "gzip -d | " + rsc
    command= rformat("{ssc} | {ssh} '{rsc}'", locals())
    execute_command(command)
    

def snapshot_subvolume( remote_host, remote_subvol_path, remote_snap_path, writable=True):
    ssh= "ssh {remote_host}"
    ro= "" if writable else "-r "
    command= "{ssh} '/sbin/btrfs subvol snap {ro}{remote_subvol_path} {remote_snap_path}'"
    execute_command(rformat(command,locals()))

def prepare_remote_subvol(remote_host, remote_subvol_path, remote_snap_dir, base=None):
    '''makes sure the remote subvolume is a writable subvolume. 
    If base is given, it will make sure that the subvolume is a 
    unmodified writable snapshot of the subvolume base. Otherwise, 
    make sure that that it's empty'''
    ssh= "ssh {remote_host}"
    execute_command( rformat("{ssh} '/sbin/btrfs subvol del {remote_subvol_path}'", locals()), check=False )
    if base:
        snapshot_subvolume( remote_host, remote_snap_dir, remote_subvol_path, base )
    else:
        execute_command( rformat("{ssh} '/sbin/btrfs subvol create {remote_subvol_path}'", locals()))
    
def open_logfile():
    if os.path.exists(LOCK_FILE):
        print "Previous transfer still running, or interrupted. Exiting"
        exit(1)
    open(LOCK_FILE, 'w').close()

def critical_error(error, message):
    print message
    exit(error)

def get_last_synced_snapshot():
    return open(LAST_SYNC_FILE).read() if os.path.exists(LAST_SYNC_FILE) else None

def get_local_snapshots():
    return sorted(os.listdir(LOCAL_SNAP_DIR))

open_logfile()
local_snaps= get_local_snapshots()
logging.debug("Existing snapshots: "+";".join(local_snaps))
if not local_snaps:
    critical_error(2,"No snapshots on the local folder")
last_sync= get_last_synced_snapshot()
logging.debug("Last sync: "+str(last_sync))
try:
    last_sync_i= local_snaps.index(last_sync) if last_sync else -1
except ValueError:
    critical_error(3, "The last synced snapshot doesn't exist")
synced_snapshots= local_snaps[:last_sync_i+1]
unsynced_snapshots= local_snaps[last_sync_i+1:]
logging.debug("Already synced: "+str(synced_snapshots))
logging.debug("Not yet synced: "+str(unsynced_snapshots))

if unsynced_snapshots:
    prepare_remote_subvol(REMOTE_HOST, REMOTE_SUBVOLUME_PATH, REMOTE_SNAP_DIR, last_sync)
for syncing_snap in unsynced_snapshots:
    logging.info("Syncing snapshot: "+syncing_snap+ (" delta from " + last_sync if last_sync else ""))
    local_snap=    os.path.join(LOCAL_SNAP_DIR,  syncing_snap)
    delta_snap=    os.path.join(LOCAL_SNAP_DIR,  last_sync   ) if last_sync else None
    remote_snap=   os.path.join(REMOTE_SNAP_DIR, syncing_snap)
    send_snapshot_to_subvol(local_snap, REMOTE_HOST, REMOTE_SUBVOLUME_PATH, last_sync)
    snapshot_subvolume(REMOTE_HOST, REMOTE_SUBVOLUME_PATH, remote_snap, writable=False)
    open(LAST_SYNC_FILE, 'w').write(syncing_snap)
os.remove(LOCK_FILE)
