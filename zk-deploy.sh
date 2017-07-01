#!/bin/bash

if [ -z "$zk_deploy_defined" ] ; then
zk_deploy_defined="true"

ZKSCRIPT="${BASH_SOURCE-$0}"
ZKSCRIPT_PDIR="$(dirname "${ZKSCRIPT}")"
ZKSCRIPT_PDIR="$(cd "${ZKSCRIPT_PDIR}"; pwd)"

. $ZKSCRIPT_PDIR/tools.sh

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
    local sshErr=`mktemp --suffix=-stor-deploy.zk_clean`

    #Step-1: try to stop running zookeeper process if found.
    local succ=""
    local zk_pid=""
    for r in {1..4} ; do
        zk_pid=`$SSH jps 2> $sshErr | grep QuorumPeerMain | cut -d ' ' -f 1 | grep '^[0-9][0-9]*$'`
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit clean_up_zknode(): failed to find running zookeepr on $node. See $sshErr for details"
            return 1
        fi

        if [ -z "$zk_pid" ] ; then
            log "INFO: in clean_up_zknode(): didn't find running zookeepr on $node"
            succ="true"
            break
        fi

        log "INFO: in clean_up_zknode(): found a running zookeepr on $node, zk_pid=$zk_pid. try to stop it..."

        $SSH systemctl stop zookeeper
        sleep 2
        $SSH kill -9 $zk_pid
        sleep 2
    done

    if [ "X$succ" != "Xtrue" ] ; then
        log "ERROR: Exit clean_up_zknode(): failed to stop zookeeper on $onde. zk_pid=$zk_pid"
        return 1
    fi

    #Step-2: try to remove the zookeeper package;
    $SSH systemctl disable zookeeper
    $SSH rm -fr $install_path/$version /usr/lib/systemd/system/zookeeper.service /etc/systemd/system/zookeeper.service
    $SSH ls $install_path/$version /usr/lib/systemd/system/zookeeper.service /etc/systemd/system/zookeeper.service > $sshErr 2>&1
    local n=`cat $sshErr | grep "No such file or directory" | wc -l`
    if [ $n -ne 3 ] ; then
        log "ERROR: Exit clean_up_zknode(): ssh failed or we failed to remove zookeeper package on $node. See $sshErr for details"
        return 1
    fi

    $SSH systemctl daemon-reload 2> $sshErr
    if [ -s "$sshErr" ] ; then
        log "ERROR: Exit clean_up_zknode(): systemctl daemon-reload failed. See $sshErr for details"
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
    log "INFO:       java_home=$java_home"
    log "INFO:       dataDir=$dataDir"
    log "INFO:       dataLogDir=$dataLogDir"
    log "INFO:       logdir=$logdir"
    log "INFO:       pidfile=$pidfile"
    log "INFO:       mounts=$mounts"
    log "INFO:       mount_opts=$mount_opts"
    log "INFO:       mkfs_cmd=$mkfs_cmd"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.ssh.err`

    #Step-1: test if java is available
    $SSH $java_home/bin/java -version > $sshErr 2>&1
    cat $sshErr | grep "java version" > /dev/null 2>&1
    if [ $? -ne 0 ] ;  then  #didn't found "java version", so java is not available
        log "ERROR: Exit prepare_zknode(): java is not availabe at $java_home on $node. See $sshErr for details"
        return 1
    fi

    #Step-2: mount the disks
    if [ -n "$mounts" ] ; then
        if [ -z "$mount_opts" -o -z "$mkfs_cmd" ] ; then
            log "ERROR: Exit prepare_zknode(): mount_opts and mkfs_cmd must be present when mounts is present"
            return 1
        fi

        do_mounts "$node" "$user" "$ssh_port" "$mounts" "$mount_opts" "$mkfs_cmd"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit prepare_zknode(): do_mounts failed for $node"
            return 1
        fi
    fi

    #Step-3: create dirs for zookeeper; 
    local piddir=`dirname $pidfile`
    $SSH mkdir -p $dataDir $dataLogDir $logdir $piddir 2> $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit prepare_zknode(): failed to create dirs for zookeeper on $node. See $sshErr for details"
        return 1
    fi

    $SSH rm -fr $dataDir/* $dataLogDir/* $logdir/* $piddir/* 2> $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit prepare_zknode(): failed to rm legacy data of zookeeper on $node. See $sshErr for details"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit prepare_zknode(): Success"
    return 0
}

function generate_zk_cfg()
{
    local zk_conf_dir=$1

    log "INFO: Enter generate_zk_cfg(): zk_conf_dir=$zk_conf_dir"

    local zk_comm_cfg=$zk_conf_dir/common
    local zk_nodes=$zk_conf_dir/nodes

    #Step-0: generate a piece of zoo.cfg:
    #          server.1=node1.localdomain:2888:3888
    #          server.2=node2.localdomain:2888:3888
    #          server.3=node3.localdomain:2888:3888

    local all_nodes_port=`mktemp --suffix=-stor-deploy.all_zk_port`
    rm -f $all_nodes_port
    local id=1
    for node in `cat $zk_nodes` ; do
        echo "server.${id}=${node}:2888:3888" >> $all_nodes_port
        id=`expr $id + 1`
    done

    #Step-1: generate zoo.cfg based on $zk_comm_cfg;
    local comm_zoo_cfg=$zk_conf_dir/zoo.cfg.common
    rm -f $comm_zoo_cfg
    cat $zk_comm_cfg  | grep "^cfg:" | while read line ; do
        line=`echo $line | sed -e 's/^cfg://'`
        echo $line >> $comm_zoo_cfg
    done
    cat $all_nodes_port >> $comm_zoo_cfg

    #Step-2:
    
    #Step-2: generate zoo.cfg based on cfg of each node, if the cfg is different from $zk_comm_cfg
    local tmpfile=`mktemp --suffix=-stor-deploy.gen_zk_cfg`
    for node in `cat $zk_nodes` ; do
        local node_specific_cfg=$zk_conf_dir/$node
        if [ ! -f $node_specific_cfg ] ; then
            log "INFO: in generate_zk_cfg(): $node has no specific config file, so its config is the same as common"
            continue
        fi

        log "INFO: in generate_zk_cfg(): $node has a specific config file"
        sed -e '/^myid=/ d' $node_specific_cfg  > $tmpfile
        local diff_cfg=`diff $tmpfile $zk_comm_cfg`
        if [ -z "$diff_cfg" ] ; then
            log "INFO: in generate_zk_cfg(): $node has a specific config file, but it's the same as common, skip it!"
            continue
        fi

        log "INFO: in generate_zk_cfg(): $node has a specific config file that's different from common, process it!"

        local node_zoo_cfg=$zk_conf_dir/zoo.cfg.$node
        rm -f $node_zoo_cfg
        cat $node_specific_cfg  | grep "^cfg:" | while read line ; do
            line=`echo $line | sed -e 's/^cfg://'`
            echo $line >> $node_zoo_cfg
        done
        cat $all_nodes_port >> $node_zoo_cfg
    done

    rm -f $all_nodes_port $tmpfile 

    log "INFO: Exit generate_zk_cfg(): Success"
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
        log "INFO:        user=$user"
        log "INFO:        ssh_port=$ssh_port"
        log "INFO:        java_home=$java_home"
        log "INFO:        install_path=$install_path"
        log "INFO:        version=$version"
        log "INFO:        mounts=$mounts"
        log "INFO:        mkfs_cmd=$mkfs_cmd"
        log "INFO:        mount_opts=$mount_opts"
        log "INFO:        dataDir=$dataDir"
        log "INFO:        dataLogDir=$dataLogDir"
        log "INFO:        pidfile=$pidfile"
        log "INFO:        logdir=$logdir"

        if [ X"$user" != "root" ] ; then
            log "ERROR: currently, only 'root' user is allowed."
            return 1
        fi

        log "INFO: in deploy_zk(): clean up zk node $node ..."
        clean_up_zknode "$node" "$user" "$ssh_port" "$install_path" "$version"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_zk(): clean_up_zknode failed on $node"
            return 1
        fi

        log "INFO: in deploy_zk(): prepare zk node $node ..."
        prepare_zknode "$node" "$user" "$ssh_port" "$java_home" "$dataDir" "$dataLogDir" "$logdir" "$pidfile" "$mounts" "$mount_opts" "$mkfs_cmd"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_zk(): prepare_zknode failed on $node"
            return 1
        fi
    done

    log "INFO: Exit deploy_zk(): Success"
    return 0
}

generate_zk_cfg logs/test2/zk

fi
