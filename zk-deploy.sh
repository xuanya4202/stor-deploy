#!/bin/bash

if [ -z "$zk_deploy_defined" ] ; then
zk_deploy_defined="true"

ZKSCRIPT="${BASH_SOURCE-$0}"
ZKSCRIPT_PDIR="$(dirname "${ZKSCRIPT}")"
ZKSCRIPT_PDIR="$(cd "${ZKSCRIPT_PDIR}"; pwd)"

. $SCRIPT_PDIR/tools.sh

function clean_up_zknode()
{
    #user must be 'root' for now, so we don't use sudo;
    local node=$1
    local user=$2
    local ssh_port=$3
    local install_path=$4
    local version=$5

    log "INFO: Enter clean_up_zknode(): node=$node user=$user ssh_port=$ssh_port install_path=$install_path version=$version"

    local SSH="ssh -p $port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.ssh.err`
    local ssh_cmd=""

    #Step-1: try to stop running zookeeper process if found.
    ssh_cmd="$SSH jps 2> $sshErr | grep QuorumPeerMain | cut -d ' ' -f 1 | grep '^[0-9][0-9]*$'"
    local zk_pid=`$ssh_cmd`
    if [ -s "$sshErr" ] ; then
        log "ERROR: Exit clean_up_zknode(): ssh failed. ssh_cmd=\"$ssh_cmd\". See $sshErr for details"
        return 1
    fi

    if [ -n "$zk_pid" ] ; then  # we found a zookeeper process;
        log "INFO: in clean_up_zknode(): try all ways to stop zookeeper"
        $SSH systemctl stop zookeeper
        sleep 2
        $SSH kill -9 $zk_pid
        sleep 3

        zk_pid=`$ssh_cmd`
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit clean_up_zknode(): ssh failed. ssh_cmd=\"$ssh_cmd\". See $sshErr for details"
            return 1
        fi

        if [ -n "$zk_pid" ] ; then
            log "ERROR: Exit clean_up_zknode(): found a zookeeper on $onde and failed to stop it"
            return 1
        else
            log "INFO: Exit clean_up_zknode(): succeeded to stop zookeeper on $onde"
        fi
    fi

    #Step-2: try to remove the zookeeper package;
    $SSH systemctl disable zookeeper
    $SSH rm -fr $install_path/$version /usr/lib/systemd/system/zookeeper.service /etc/systemd/system/zookeeper.service

    ssh_cmd="$SSH ls $install_path/$version /usr/lib/systemd/system/zookeeper.service /etc/systemd/system/zookeeper.service > $sshErr 2>&1"
    $ssh_cmd
    local n=`cat $sshErr | grep "No such file or directory" | wc -l`
    if [ $n -ne 3 ] ; then
        log "ERROR: Exit clean_up_zknode(): ssh failed. ssh_cmd=\"$ssh_cmd\". See $sshErr for details"
        return 1
    fi

    ssh_cmd="$SSH systemctl daemon-reload" 2> $sshErr
    $ssh_cmd
    if [ -s "$sshErr" ] ; then
        log "ERROR: Exit clean_up_zknode(): ssh failed. ssh_cmd=\"$ssh_cmd\". See $sshErr for details"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit clean_up_zknode(): Success"
    return 0
}

function prepare_zknode()
{
    #user must be 'root' for now, so we don't use sudo;
    local node=$1
    local user=$2
    local ssh_port=$3
    local java_home=$4
    local dataDir=$5
    local dataLogDir=$6
    local logdir=$7
    local pidfile=$8

    shift 8

    local mounts=$1
    local mount_opts=$2
    local mkfs_cmd=$3

    log "INFO: Enter prepare_zknode(): node=$node user=$user ssh_port=$ssh_port"
    log "INFO:     java_home=$java_home"
    log "INFO:     dataDir=$dataDir"
    log "INFO:     dataLogDir=$dataLogDir"
    log "INFO:     logdir=$logdir"
    log "INFO:     pidfile=$pidfile"
    log "INFO:     mounts=$mounts"
    log "INFO:     mount_opts=$mount_opts"
    log "INFO:     mkfs_cmd=$mkfs_cmd"

    local SSH="ssh -p $port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.ssh.err`
    local ssh_cmd=""

    #Step-1: test if java is available
    $SSH $java_home/bin/java -version > $sshErr 2>&1
    cat $sshErr | grep "java version" > /dev/null 2>&1
    if [ $? -ne 0 ] ;  then  #didn't found "java version", so java is not available
        log "ERROR: Exit prepare_zknode(): java is not availabe at $java_home on $node. See $sshErr for details"
        return 1
    fi

    #Step-2: mount the disk
    if [ -n "$mounts" ] ; then
        if [ -z "$mount_opts" -o -z "$mkfs_cmd" ] ; then
            log "ERROR: Exit prepare_zknode(): mount_opts and mkfs_cmd must be present when mounts is present"
            return 1
        fi

        log "INFO: in prepare_zknode(): "
    fi

    rm -f $sshErr

    log "INFO: Exit prepare_zknode(): Success"
    return 0
}

function deploy_zk()
{
    local parsed_conf_dir=$1

    log "INFO: Enter deploy_zk(): parsed_conf_dir=$parsed_conf_dir"

    local zk_conf_dir=$parsed_conf_dir/zk
    local zk_comm_cfg=$zk_conf_dir/common
    local zk_nodes=$zk_conf_dir/nodes

    if [ ! -d $zk_conf_dir ] ; then
        log "ERROR: Exit deploy_zk(): dir $zk_conf_dir does not exist"
        return 1
    fi

    if [ ! -f $zk_comm_cfg -o ! -f $zk_nodes ] ; then
        log "ERROR: Exit deploy_zk(): file $zk_comm_cfg or $zk_nodes does not exist"
        return 1
    fi

    for node in `cat $zk_nodes` ; do
        local node_cfg=$zk_comm_cfg
        [ -f $zk_conf_dir/$node ] ; node_cfg=$zk_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local java_home=`grep "env:JAVA_HOME=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local version=`grep "version=" $node_cfg | cut -d '=' -f 2-`
        local mounts=`grep "mounts=" $node_cfg | cut -d '=' -f 2-`
        local mkfs_cmd=`grep "mkfs_cmd=" $node_cfg | cut -d '=' -f 2-`
        local mount_opts=`grep "mount_opts=" $node_cfg | cut -d '=' -f 2-`
        local dataDir=`grep "cfg:dataDir=" $node_cfg | cut -d '=' -f 2-`
        local dataLogDir=`grep "cfg:dataLogDir=" $node_cfg | cut -d '=' -f 2-`
        local pidfile=`grep "env:ZOOPIDFILE=" $node_cfg | cut -d '=' -f 2-`
        local logdir=`grep "env:ZOO_LOG_DIR=" $node_cfg | cut -d '=' -f 2-`

        log "INFO: in deploy_zk(): node=$node node_cfg=$node_cfg"
        log "INFO: in deploy_zk():         user=$user"
        log "INFO: in deploy_zk():         ssh_port=$ssh_port"
        log "INFO: in deploy_zk():         java_home=$java_home"
        log "INFO: in deploy_zk():         install_path=$install_path"
        log "INFO: in deploy_zk():         version=$version"
        log "INFO: in deploy_zk():         mounts=$mounts"
        log "INFO: in deploy_zk():         mkfs_cmd=$mkfs_cmd"
        log "INFO: in deploy_zk():         mount_opts=$mount_opts"
        log "INFO: in deploy_zk():         dataDir=$dataDir"
        log "INFO: in deploy_zk():         dataLogDir=$dataLogDir"
        log "INFO: in deploy_zk():         pidfile=$pidfile"
        log "INFO: in deploy_zk():         logdir=$logdir"

        if [ X"$user" != "root" ] ; then
            log "ERROR: currently, only 'root' user is allowed."
            return 1
        fi

        log "INFO: in deploy_zk(): clean up zk node $node ..."
        clean_up_zknode "$node" "$user" "$ssh_port" "$install_path" "$version"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_zk(): failed to clean up zk node $node"
        fi

        prepare_zknode "$node" "$user" "$ssh_port" "$java_home" "$dataDir" "$dataLogDir" "$logdir" "$pidfile" "$mounts" "$mount_opts" "$mkfs_cmd"


    done
}

fi
