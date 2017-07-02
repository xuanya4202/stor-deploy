#!/bin/bash

if [ -z "$zk_deploy_defined" ] ; then
zk_deploy_defined="true"

ZKSCRIPT="${BASH_SOURCE-$0}"
ZKSCRIPT_PDIR="$(dirname "${ZKSCRIPT}")"
ZKSCRIPT_PDIR="$(cd "${ZKSCRIPT_PDIR}"; pwd)"

. $ZKSCRIPT_PDIR/tools.sh

[-z "$run_timestamp" ] && run_timestamp=`date +%Y%m%d%H%M%S`

function check_zknode()
{
    #user must be 'root' for now, so we don't use sudo;
    local node=$1
    local user=$2
    local ssh_port=$3
    local java_home=$4

    log "INFO: Enter check_zknode(): node=$node user=$user ssh_port=$ssh_port java_home=$java_home"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.check`

    $SSH $java_home/bin/java -version > $sshErr 2>&1
    cat $sshErr | grep "java version" > /dev/null 2>&1
    if [ $? -ne 0 ] ;  then  #didn't found "java version", so java is not available
        log "ERROR: Exit check_zknode(): java is not availabe at $java_home on $node. See $sshErr for details"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit check_zknode(): Success"
    return 0
}

function clean_up_zknode()
{
    #user must be 'root' for now, so we don't use sudo;
    local node=$1
    local user=$2
    local ssh_port=$3
    local installation=$4

    log "INFO: Enter clean_up_zknode(): node=$node user=$user ssh_port=$ssh_port installation=$installation"

    local SSH="ssh -p $ssh_port $user@$node"
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

    #Step-2: try to remove the legacy zookeeper installation;
    log "INFO: in clean_up_zknode(): remove legacy zookeeper installation if there is on $node"
    local backup=/tmp/zookeeper-backup-$run_timestamp
    $SSH "systemctl disable zookeeper ; mkdir -p $backup ; mv -f $installation /usr/lib/systemd/system/zookeeper.service /etc/systemd/system/zookeeper.service $backup ; systemctl daemon-reload" 2> /dev/null
    $SSH ls $installation /usr/lib/systemd/system/zookeeper.service /etc/systemd/system/zookeeper.service > $sshErr 2>&1
    local n=`cat $sshErr | grep "No such file or directory" | wc -l`
    if [ $n -ne 3 ] ; then
        log "ERROR: Exit clean_up_zknode(): ssh failed or we failed to remove legacy zookeeper installation on $node. See $sshErr for details"
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
    local dataDir=$4
    local dataLogDir=$5
    local logdir=$6
    local pidfile=$7
    local install_path=$8

    shift 8

    local mounts=$1
    local mount_opts=$2
    local mkfs_cmd=$3

    log "INFO: Enter prepare_zknode(): node=$node user=$user ssh_port=$ssh_port"
    log "INFO:       dataDir=$dataDir"
    log "INFO:       dataLogDir=$dataLogDir"
    log "INFO:       logdir=$logdir"
    log "INFO:       pidfile=$pidfile"
    log "INFO:       install_path=$install_path"
    log "INFO:       mounts=$mounts"
    log "INFO:       mount_opts=$mount_opts"
    log "INFO:       mkfs_cmd=$mkfs_cmd"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.prepare_zknode`

    #Step-1: mount the disks
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

    #Step-2: create dirs for zookeeper; 
    local piddir=`dirname $pidfile`
    $SSH "mkdir -p $dataDir $dataLogDir $logdir $piddir $install_path" 2> $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit prepare_zknode(): failed to create dirs for zookeeper on $node. See $sshErr for details"
        return 1
    fi

    $SSH rm -fr $dataDir/* $dataLogDir/* $logdir/* $piddir/* 2> $sshErr #don't rm $install_path/* (e.g. /usr/local/)
    if [ -s $sshErr ] ; then
        log "ERROR: Exit prepare_zknode(): failed to rm legacy data of zookeeper on $node. See $sshErr for details"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit prepare_zknode(): Success"
    return 0
}

function dispatch_zk_package()
{
    local zk_conf_dir=$1

    log "INFO: Enter dispatch_zk_package(): zk_conf_dir=$zk_conf_dir"

    local zk_comm_cfg=$zk_conf_dir/common
    local zk_nodes=$zk_conf_dir/nodes

    local src_md5=""
    local base_name=""
    for node in `cat $zk_nodes` ; do
        local node_cfg=$zk_comm_cfg
        [ -f $zk_conf_dir/$node ] ; node_cfg=$zk_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

        [ -z "$src_md5" ] && src_md5=`md5sum $package | cut -d ' ' -f 1`
        [ -z "$base_name" ] && base_name=`basename $package`

        log "INFO: in dispatch_zk_package(): start background task: scp -P $ssh_port $package $user@$node:$install_path"
        scp -P $ssh_port $package $user@$node:$install_path &
    done

    wait

    local sshErr=`mktemp --suffix=-stor-deploy.zk_dispatch`
    for node in `cat $zk_nodes` ; do
        local node_cfg=$zk_comm_cfg
        [ -f $zk_conf_dir/$node ] ; node_cfg=$zk_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

        local SSH="ssh -p $ssh_port $user@$node"

        local dst_md5=`$SSH md5sum $install_path/$base_name 2> $sshErr | cut -d ' ' -f 1`
        if [ -s $sshErr ] ; then
            log "ERROR: Exit dispatch_zk_package(): failed to get md5sum of $install_path/$base_name on $node. See $sshErr for details"
            return 1
        fi

        if [ "X$dst_md5" != "X$src_md5" ] ; then
            log "ERROR: Exit dispatch_zk_package(): md5sum of $install_path/$base_name on $node is incorrect. src_md5=$src_md5 dst_md5=$dst_md5"
            return 1
        fi

        $SSH tar zxf $install_path/$base_name -C $install_path 2> $sshErr
        if [ -s $sshErr ] ; then
            log "ERROR: Exit dispatch_zk_package(): failed to extract $install_path/$base_name on $node. See $sshErr for details"
            return 1
        fi
    done

    rm -f $sshErr

    log "INFO: Exit dispatch_zk_package(): Success"
    return 0
}

#generate config files for 'common' or a specific node;
#Notice that the zoo.cfg.$Node generated is not complete;
function gen_cfg_for_node()
{
    local zk_conf_dir=$1
    local node=$2    # node may be a specific node (such as 192.168.100.131), or 'common'
    local zoo_cfg_node_list=$3

    log "INFO: Enter gen_cfg_for_node(): zk_conf_dir=$zk_conf_dir node=$node"

    local node_cfg=$zk_conf_dir/$node

    #Step-1: check if specific config file exists for a node. For zookeeper, it must exist;
    if [ "X$node" != "Xcommon" ] ; then
        if [ ! -f $node_cfg ] ; then
            log "ERROR: Exit gen_cfg_for_node(): $node does not have a specific config file"
            return 1
        else
            log "INFO: in gen_cfg_for_node(): $node has a specific config file"
        fi
    fi

    #Step-2: generate 'myid' file if node is not 'common'
    if [ "X$node" != "Xcommon" ] ; then
        local myid_file=$zk_conf_dir/myid.$node
        rm -f $myid_file

        local myid=`grep "myid=" $node_cfg | cut -d '=' -f 2-`
        if [ -z "$myid" ] ; then
            log "ERROR: Exit gen_cfg_for_node(): there is no myid in $node_cfg"
            return 1
        fi

        if [[ ! "$myid" =~ ^[0-9][0-9]*$ ]] ; then
            log "ERROR: Exit gen_cfg_for_node(): myid in $node_cfg is invalid. myid=$myid"
            return 1
        fi

        echo $myid > $myid_file || return 1

        echo "server.${myid}=${node}:2888:3888" >> $zoo_cfg_node_list || return 1
    fi

    #Step-3: check if the config file for $node is the same as common
    if [ "X$node" != "Xcommon" ] ; then
        local tmpfile=`mktemp --suffix=-stor-deploy.gen_zk_cfg`
        local comm_cfg=$zk_conf_dir/common

        sed -e '/^myid=/ d' $node_cfg > $tmpfile

        local cfg_diff=`diff $tmpfile $comm_cfg`
        if [ -z "$cfg_diff" ] ; then
            log "INFO: Exit gen_cfg_for_node(): specific config file of $node is same as common, skip it!"
            rm -f $tmpfile
            return 0
        else
            log "INFO: in gen_cfg_for_node(): specific config file of $node is different from common, process it!"
            rm -f $tmpfile
        fi
    fi

    #Step-4: generate zoo.cfg.$node
    local zoo_cfg=$zk_conf_dir/zoo.cfg.$node
    rm -f $zoo_cfg
    cat $node_cfg | grep "^cfg:" | while read line ; do
        line=`echo $line | sed -e 's/^cfg://'`
        echo $line >> $zoo_cfg
    done

    #Step-5: generate zkEnv.sh.$node
    #we copy the release version and make some changes;
    #Note that in function dispatch_zk_package, we have already dispatched the zookeeper package to each node, and 
    #extracted them in install_path. So we copy the zkEnv.sh from current node;

    local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
    local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`
    local installation=`basename $package`
    installation=`echo $installation | sed -e 's/.tar.gz$//' -e 's/.tgz$//'`
    installation=$install_path/$installation
    
    if [ ! -f $installation/bin/zkEnv.sh ] ; then
        log "ERROR: Exit gen_cfg_for_node(): $installation/bin/zkEnv.sh doesn't exist"
        return 1
    fi

    local zk_env=$zk_conf_dir/zkEnv.sh.$node
    rm -f $zk_env
    cp -f $installation/bin/zkEnv.sh $zk_env

    cat $node_cfg | grep "^env:" | while read line ; do
        line=`echo $line | sed -e 's/^env://'`
        sed -i -e "2 i $line" $zk_env
    done

    log "INFO: Exit gen_cfg_for_node(): Success"
    return 0
}

function gen_cfg_for_all_nodes()
{
    local zk_conf_dir=$1

    log "INFO: Enter gen_cfg_for_all_nodes(): zk_conf_dir=$zk_conf_dir"

    local zk_nodes=$zk_conf_dir/nodes

    #generate config files for common
    gen_cfg_for_node "$zk_conf_dir" "common"
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit gen_cfg_for_all_nodes(): failed to generate config files for 'common'"
        return 1
    fi

    #generate config files for specific nodes 
    local zoo_cfg_node_list=`mktemp --suffix=-stor-deploy.zoo_cfg_list`
    rm -f $zoo_cfg_node_list
    for node in `cat $zk_nodes` ; do
        gen_cfg_for_node "$zk_conf_dir" "$node" "$zoo_cfg_node_list"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit gen_cfg_for_all_nodes(): failed to generate config files for $node"
            return 1
        fi
    done

    #Notice that, in gen_cfg_for_node() the zoo.cfg.$node generated is not complete, a list like this is missing:
    #          server.1=node1.localdomain:2888:3888
    #          server.2=node2.localdomain:2888:3888
    #          server.3=node3.localdomain:2888:3888
    #however, we have gathered the list in $zoo_cfg_node_list
    for zoo_cfg in `find $zk_conf_dir -name "zoo.cfg.*" -type f` ; do
        cat $zoo_cfg_node_list >> $zoo_cfg
    done

    rm -f $zoo_cfg_node_list

    log "INFO: Exit gen_cfg_for_all_nodes(): Success"
    return 0
}

function dispatch_zk_configs()
{
    local zk_conf_dir=$1

    log "INFO: Enter dispatch_zk_configs(): zk_conf_dir=$zk_conf_dir"

    local zk_comm_cfg=$zk_conf_dir/common
    local zk_nodes=$zk_conf_dir/nodes

    for node in `cat $zk_nodes` ; do
        local node_cfg=$zk_comm_cfg
        [ -f $zk_conf_dir/$node ] ; node_cfg=$zk_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`
        local dataDir=`grep "cfg:dataDir=" $node_cfg | cut -d '=' -f 2-`

        log "INFO: in dispatch_zk_configs(): node=$node node_cfg=$node_cfg"
        log "INFO:        user=$user"
        log "INFO:        ssh_port=$ssh_port"
        log "INFO:        install_path=$install_path"
        log "INFO:        package=$package"
        log "INFO:        dataDir=$dataDir"

        local installation=`basename $package`
        installation=`echo $installation | sed -e 's/.tar.gz$//' -e 's/.tgz$//'`
        installation=$install_path/$installation

        local zoo_cfg=$zk_conf_dir/zoo.cfg.common
        [ -f $zk_conf_dir/zoo.cfg.$node ] && zoo_cfg=$zk_conf_dir/zoo.cfg.$node

        local zkEnv_sh=$zk_conf_dir/zkEnv.sh.common
        [ -f $zk_conf_dir/zkEnv.sh.$node ] && zkEnv_sh=$zk_conf_dir/zkEnv.sh.$node

        local myid_file=$zk_conf_dir/myid.$node

        log "INFO: in dispatch_zk_configs(): for $node: zoo_cfg=$zoo_cfg zkEnv_sh=$zkEnv_sh myid_file=$myid_file"


        if [ ! -f $zoo_cfg -o ! -f $zkEnv_sh -o ! -f $myid_file ] ; then
            log "ERROR: Exit dispatch_zk_configs(): file $zoo_cfg or $zkEnv_sh or $myid_file does not exist"
            return 1
        fi

        local SCP="scp -P $ssh_port"
        $SCP $zoo_cfg $user@$node:$installation/conf/zoo.cfg
        $SCP $zkEnv_sh $user@$node:$installation/bin/zkEnv.sh
        $SCP $myid_file $user@$node:$dataDir/myid

        local SSH="ssh -p $ssh_port $user@$node"
        local sshErr=`mktemp --suffix=-stor-deploy.dispatch_cfg`

        local remoteMD5=`mktemp --suffix=-stor-deploy.remoteMD5`
        $SSH md5sum $installation/conf/zoo.cfg $installation/bin/zkEnv.sh $dataDir/myid 2> $sshErr | cut -d ' ' -f 1 > $remoteMD5

        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit dispatch_zk_configs(): failed to get md5 of config files on $node. See $sshErr for details"
            return 1
        fi

        local localMD5=`mktemp --suffix=-stor-deploy.localMD5`
        md5sum $zoo_cfg $zkEnv_sh $myid_file | cut -d ' ' -f 1 > $localMD5

        local md5Diff=`diff $remoteMD5 $localMD5`
        if [ -n "$md5Diff" ] ; then
            log "ERROR: Exit dispatch_zk_configs(): md5 of config files on $node is incorrect. See $sshErr, $remoteMD5 and $localMD5 for details"
            return 1
        fi

        rm -f $sshErr $remoteMD5 $localMD5
    done

    log "INFO: Exit dispatch_zk_configs(): Success"
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

    #Step-1: check zk nodes (such as java environment), clean up zk nodes, and prepare (mount the disks, create the dirs)
    for node in `cat $zk_nodes` ; do
        local node_cfg=$zk_comm_cfg
        [ -f $zk_conf_dir/$node ] ; node_cfg=$zk_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local java_home=`grep "env:JAVA_HOME=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`
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
        log "INFO:        package=$package"
        log "INFO:        mounts=$mounts"
        log "INFO:        mkfs_cmd=$mkfs_cmd"
        log "INFO:        mount_opts=$mount_opts"
        log "INFO:        dataDir=$dataDir"
        log "INFO:        dataLogDir=$dataLogDir"
        log "INFO:        pidfile=$pidfile"
        log "INFO:        logdir=$logdir"

        if [ "X$user" != "Xroot" ] ; then
            log "ERROR: Exit deploy_zk(): currently, only 'root' user is allowed. user=$user"
            return 1
        fi

        if [ -z "$package" ] ; then
            log "ERROR: Exit deploy_zk(): package must be configured, but it's empty"
            return 1
        fi

        if [[ "$package" =~ .tar.gz$ ]] || [[ "$package" =~ .tgz$ ]] ; then
            log "INFO: in deploy_zk(): zookeeper package name looks good. package=$package"
        else
            log "ERROR: Exit deploy_zk(): zookeeper package must be tar.gz or tgz package. package=$package"
            return 1
        fi

        if [ ! -f $package ] ; then
            log "ERROR: Exit deploy_zk(): zookeeper package doesn't exist. package=$package"
            return 1
        fi

        local installation=`basename $package`
        installation=`echo $installation | sed -e 's/.tar.gz$//' -e 's/.tgz$//'`
        installation=$install_path/$installation

        #check the node, such as java environment
        log "INFO: in deploy_zk(): check zk node $node ..."
        check_zknode "$node" "$user" "$ssh_port" "$java_home"

        #clean up zk node
        log "INFO: in deploy_zk(): clean up zk node $node ..."
        clean_up_zknode "$node" "$user" "$ssh_port" "$installation"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_zk(): clean_up_zknode failed on $node"
            return 1
        fi

        #prepare environment
        log "INFO: in deploy_zk(): prepare zk node $node ..."
        prepare_zknode "$node" "$user" "$ssh_port" "$dataDir" "$dataLogDir" "$logdir" "$pidfile" "$install_path" "$mounts" "$mount_opts" "$mkfs_cmd"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_zk(): prepare_zknode failed on $node"
            return 1
        fi
    done

    #Step-2: dispatch zk package to each node. Note that what's dispatched is the release-package, which doesn't
    #        contain our configurations. We will dispatch the configuation files later.
    dispatch_zk_package $zk_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_zk(): failed to dispatch zookeeper package to some node"
        return 1
    fi

    #Step-3: generate configurations for each zk node;
    gen_cfg_for_all_nodes $zk_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_zk(): failed to generate configuration files for each node"
        return 1
    fi

    #Step-4: dispatch configurations to each zk node;
    dispatch_zk_configs $zk_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_zk(): failed to dispatch configuration files to each node"
        return 1
    fi

    #Step-5: start zookeeper servers on each zk node;
    start_zk_servers $zk_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_zk(): failed to start zookeeper server on each node"
        return 1
    fi

    #Step-6: check zookeeper status on each zk node;
    check_zk_status $zk_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_zk(): failed to check zookeeper status on each node"
        return 1
    fi

    log "INFO: Exit deploy_zk(): Success"
    return 0
}

fi
