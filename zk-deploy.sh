#!/bin/bash

if [ -z "$zk_deploy_defined" ] ; then
zk_deploy_defined="true"

ZKSCRIPT="${BASH_SOURCE-$0}"
ZKSCRIPT_PDIR="$(dirname "${ZKSCRIPT}")"
ZKSCRIPT_PDIR="$(cd "${ZKSCRIPT_PDIR}"; pwd)"

. $ZKSCRIPT_PDIR/tools.sh

[ -z "$run_timestamp" ] && run_timestamp=`date +%Y%m%d%H%M%S`

function check_zk_node()
{
    #user must be 'root' for now, so we don't use sudo;
    local node=$1
    local user=$2
    local ssh_port=$3
    local java_home=$4

    log "INFO: Enter check_zk_node(): node=$node user=$user ssh_port=$ssh_port java_home=$java_home"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.check`

    $SSH $java_home/bin/java -version > $sshErr 2>&1
    cat $sshErr | grep "java version" > /dev/null 2>&1
    if [ $? -ne 0 ] ;  then  #didn't found "java version", so java is not available
        log "ERROR: Exit check_zk_node(): java is not availabe at $java_home on $node. See $sshErr for details"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit check_zk_node(): Success"
    return 0
}

function stop_zk()
{
    #user must be 'root' for now, so we don't use sudo;
    local node=$1
    local user=$2
    local ssh_port=$3

    log "INFO: Enter stop_zk(): node=$node user=$user ssh_port=$ssh_port"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.zk_stop`

    local succ=""
    local zk_pid=""

    #Step-1: stop
    for r in {1..10} ; do
        zk_pid=`$SSH jps 2> $sshErr | grep QuorumPeerMain | cut -d ' ' -f 1`
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit stop_zk(): failed to find running zookeepr on $node. See $sshErr for details"
            return 1
        fi

        if [ -z "$zk_pid" ] ; then
            $SSH "systemctl status zookeepr" | grep "Active: active (running)" > /dev/null 2>&1
            if [ $? -ne 0 ] ; then # didn't find
                log "INFO: in stop_zk(): didn't find running zookeepr on $node"
                succ="true"
                break
            fi
        fi

        log "INFO: $SSH systemctl stop zookeeper retry=$r"
        $SSH "systemctl stop zookeeper" > /dev/null 2>&1
        sleep 2
    done

    #Step-2: try all means to stop zookeeper if Step-1 failed 
    if [ "X$succ" != "Xtrue" ] ; then
        for r in {1..10} ; do
            zk_pid=`$SSH jps 2> $sshErr | grep QuorumPeerMain | cut -d ' ' -f 1`
            if [ -s "$sshErr" ] ; then
                log "ERROR: Exit stop_zk(): failed to find running zookeepr on $node. See $sshErr for details"
                return 1
            fi

            if [ -z "$zk_pid" ] ; then
                $SSH "systemctl status zookeepr" | grep "Active: active (running)" > /dev/null 2>&1
                if [ $? -ne 0 ] ; then # didn't find
                    log "INFO: in stop_zk(): didn't find running zookeepr on $node"
                    succ="true"
                    break
                fi
            fi

            log "INFO: in stop_zk(): try to stop zookeeper by kill"
            $SSH kill -9 $zk_pid 2> /dev/null
            sleep 2
        done
    fi

    if [ "X$succ" != "Xtrue" ] ; then
        log "ERROR: Exit stop_zk(): failed to stop zookeeper on $onde. zk_pid=$zk_pid"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit stop_zk(): Success"
    return 0
}

function uninstall_zk()
{
    #user must be 'root' for now, so we don't use sudo;
    local node=$1
    local user=$2
    local ssh_port=$3
    local installation=$4

    log "INFO: Enter uninstall_zk(): node=$node user=$user ssh_port=$ssh_port installation=$installation"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.zk_uninstall`

    #Step-1: try to remove the legacy zookeeper installation;
    log "INFO: in uninstall_zk(): remove legacy zookeeper installation if there is on $node"
    local backup=/tmp/zookeeper-backup-$run_timestamp

    log "INFO: in uninstall_zk(): disable zookeeper: $SSH systemctl disable zookeeper"
    $SSH systemctl disable zookeeper 2> /dev/null

    $SSH "mkdir -p $backup ; mv -f $installation /usr/lib/systemd/system/zookeeper.service /etc/systemd/system/zookeeper.service $backup" 2> /dev/null

    log "INFO: in uninstall_zk(): reload daemon: $SSH systemctl daemon-reload"
    $SSH systemctl daemon-reload 2> $sshErr 
    if [ -s "$sshErr" ] ; then
        log "ERROR: Exit uninstall_zk(): failed to reload daemon on $node. See $sshErr for details"
        return 1
    fi

    $SSH ls $installation /usr/lib/systemd/system/zookeeper.service /etc/systemd/system/zookeeper.service > $sshErr 2>&1
    local n=`cat $sshErr | grep "No such file or directory" | wc -l`
    if [ $n -ne 3 ] ; then
        log "ERROR: Exit uninstall_zk(): ssh failed or we failed to remove legacy zookeeper installation on $node. See $sshErr for details"
        return 1
    fi

    $SSH systemctl status zookeeper 2>&1 | grep -e "missing the instance name" -e "could not be found" -e "Loaded: not-found" > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit uninstall_zk(): failed to check zookeeper.service on $node"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit uninstall_zk(): Success"
    return 0
}

function prepare_zk_node()
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

    log "INFO: Enter prepare_zk_node(): node=$node user=$user ssh_port=$ssh_port"
    log "INFO:       dataDir=$dataDir"
    log "INFO:       dataLogDir=$dataLogDir"
    log "INFO:       logdir=$logdir"
    log "INFO:       pidfile=$pidfile"
    log "INFO:       install_path=$install_path"
    log "INFO:       mounts=$mounts"
    log "INFO:       mount_opts=$mount_opts"
    log "INFO:       mkfs_cmd=$mkfs_cmd"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.prepare_zk_node`

    #Step-1: mount the disks
    if [ -n "$mounts" ] ; then
        if [ -z "$mount_opts" -o -z "$mkfs_cmd" ] ; then
            log "ERROR: Exit prepare_zk_node(): mount_opts and mkfs_cmd must be present when mounts is present"
            return 1
        fi

        do_mounts "$node" "$user" "$ssh_port" "$mounts" "$mount_opts" "$mkfs_cmd"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit prepare_zk_node(): do_mounts failed for $node"
            return 1
        fi
    fi

    #Step-2: create dirs for zookeeper; 
    local piddir=`dirname $pidfile`
    log "INFO: $SSH mkdir -p $dataDir $dataLogDir $logdir $piddir $install_path"
    $SSH "mkdir -p $dataDir $dataLogDir $logdir $piddir $install_path" 2> $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit prepare_zk_node(): failed to create dirs for zookeeper on $node. See $sshErr for details"
        return 1
    fi

    log "INFO: $SSH find $dataDir | grep -v $dataDir\$ | grep -v myid | xargs rm -fr ; rm -fr $dataLogDir/*"
    $SSH "find $dataDir | grep -v $dataDir\$ | grep -v myid | xargs rm -fr ; rm -fr $dataLogDir/*" 2> $sshErr #don't rm $install_path/* (e.g. /usr/local/)
    if [ -s $sshErr ] ; then
        log "ERROR: Exit prepare_zk_node(): failed to rm legacy data of zookeeper on $node. See $sshErr for details"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit prepare_zk_node(): Success"
    return 0
}

function dispatch_zk_package()
{
    local zk_conf_dir=$1

    log "INFO: Enter dispatch_zk_package(): zk_conf_dir=$zk_conf_dir"

    local zk_comm_cfg=$zk_conf_dir/common
    local zk_nodes=$zk_conf_dir/nodes

    local package=""
    local src_md5=""
    local base_name=""

    for node in `cat $zk_nodes` ; do
        local node_cfg=$zk_comm_cfg
        [ -f $zk_conf_dir/$node ] && node_cfg=$zk_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`

        if [ -z "$package" ] ; then  
            package=`grep "package=" $node_cfg | cut -d '=' -f 2-`
            src_md5=`md5sum $package | cut -d ' ' -f 1`
            base_name=`basename $package`
        else
            local thePack=`grep "package=" $node_cfg | cut -d '=' -f 2-`
            local the_base=`basename $thePack`
            if [ "$the_base" != "$base_name" ] ; then
                log "ERROR: Exit dispatch_zk_package(): package for $node is different from others. thePack=$thePack package=$package"
                return 1
            fi
        fi

        log "INFO: in dispatch_zk_package(): start background task: scp -P $ssh_port $package $user@$node:$install_path"
        scp -P $ssh_port $package $user@$node:$install_path &
    done

    wait

    local sshErr=`mktemp --suffix=-stor-deploy.zk_dispatch`
    for node in `cat $zk_nodes` ; do
        local node_cfg=$zk_comm_cfg
        [ -f $zk_conf_dir/$node ] && node_cfg=$zk_conf_dir/$node

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

        log "INFO: in dispatch_zk_package(): start background task: $SSH tar zxf $install_path/$base_name -C $install_path"
        $SSH tar zxf $install_path/$base_name -C $install_path 2> $sshErr &
        if [ -s $sshErr ] ; then
            log "ERROR: Exit dispatch_zk_package(): failed to extract $install_path/$base_name on $node. See $sshErr for details"
            return 1
        fi
    done

    wait

    rm -f $sshErr

    log "INFO: Exit dispatch_zk_package(): Success"
    return 0
}

#generate config files for 'common' or a specific node;
#Notice that the zoo.cfg.$Node generated is not complete;
function gen_cfg_for_zk_node()
{
    local zk_conf_dir=$1
    local node=$2    # node may be a specific node (such as 192.168.100.131), or 'common'
    local zoo_cfg_node_list=$3

    log "INFO: Enter gen_cfg_for_zk_node(): zk_conf_dir=$zk_conf_dir node=$node"

    local node_cfg=$zk_conf_dir/$node

    #Step-1: check if specific config file exists for a node. For zookeeper, it must exist;
    if [ "X$node" != "Xcommon" ] ; then
        if [ ! -f $node_cfg ] ; then
            log "ERROR: Exit gen_cfg_for_zk_node(): $node does not have a specific config file"
            return 1
        else
            log "INFO: in gen_cfg_for_zk_node(): $node has a specific config file"
        fi
    fi

    #Step-2: generate 'myid' file if node is not 'common'
    if [ "X$node" != "Xcommon" ] ; then
        local myid_file=$zk_conf_dir/myid.$node
        rm -f $myid_file

        local myid=`grep "myid=" $node_cfg | cut -d '=' -f 2-`
        if [ -z "$myid" ] ; then
            log "ERROR: Exit gen_cfg_for_zk_node(): there is no myid in $node_cfg"
            return 1
        fi

        if [[ ! "$myid" =~ ^[0-9][0-9]*$ ]] ; then
            log "ERROR: Exit gen_cfg_for_zk_node(): myid in $node_cfg is invalid. myid=$myid"
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
            log "INFO: Exit gen_cfg_for_zk_node(): specific config file of $node is same as common, skip it!"
            rm -f $tmpfile
            return 0
        else
            log "INFO: in gen_cfg_for_zk_node(): specific config file of $node is different from common, process it!"
            rm -f $tmpfile
        fi
    fi

    #Step-4: generate zoo.cfg.$node
    local zoo_cfg=$zk_conf_dir/zoo.cfg.$node
    rm -f $zoo_cfg
    cat $node_cfg | grep "^cfg:" | while read line ; do
        line=`echo $line | sed -e 's/^cfg://'`

        echo $line | grep "=" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit gen_cfg_for_zk_node(): config cfg:$line is invalid"
            return 1
        fi

        local key=`echo $line | cut -d '=' -f 1`
        local val=`echo $line | cut -d '=' -f 2-`

        if [ -z "$key" -o -z "$val" ] ; then
            log "WARN: in gen_cfg_for_zk_node(): key or val is empty: line=cfg:$line key=$key val=$val"
            continue
        fi

        echo $line >> $zoo_cfg || return 1
    done

    #Step-5: extract the package 
    local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

    if [ -z "$package" ] ; then
        log "ERROR: Exit gen_cfg_for_zk_node(): package must be configured, but it's empty"
        return 1
    fi

    if [[ "$package" =~ .tar.gz$ ]] || [[ "$package" =~ .tgz$ ]] ; then
        log "INFO: in gen_cfg_for_zk_node(): zookeeper package name looks good. package=$package"
    else
        log "ERROR: Exit gen_cfg_for_zk_node(): zookeeper package must be tar.gz or tgz package. package=$package"
        return 1
    fi

    if [ ! -s $package ] ; then
        log "ERROR: Exit gen_cfg_for_zk_node(): zookeeper package doesn't exist or is empty. package=$package"
        return 1
    fi

    local zkdir=`basename $package`
    zkdir=`echo $zkdir | sed -e 's/.tar.gz$//' -e 's/.tgz$//'`

    local extract_dir=/tmp/zk-extract-$run_timestamp

    local src_zk_env=$extract_dir/$zkdir/bin/zkEnv.sh

    if [ ! -s $src_zk_env ] ; then 
        log "INFO: in gen_cfg_for_zk_node(): unzip zookeeper package. package=$package ..."

        rm -fr $extract_dir || return 1
        mkdir -p $extract_dir || return 1

        tar -C $extract_dir -zxf $package $zkdir/bin/zkEnv.sh || return 1

        if [ ! -s $src_zk_env ] ; then 
            log "ERROR: Exit gen_cfg_for_zk_node(): failed to unzip zookeeper package. package=$package extract_dir=$extract_dir"
            return 1
        fi
    else
        log "INFO: in gen_cfg_for_zk_node(): zookeeper package has already been unzipped. package=$package"
    fi

    #Step-6: generate zkEnv.sh.$node
    local zk_env=$zk_conf_dir/zkEnv.sh.$node
    rm -f $zk_env
    cp -f $src_zk_env $zk_env || return 1

    cat $node_cfg | grep "^env:" | while read line ; do
        line=`echo $line | sed -e 's/^env://'`

        echo $line | grep "=" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit gen_cfg_for_zk_node(): config env:$line is invalid"
            return 1
        fi

        local key=`echo $line | cut -d '=' -f 1`
        local val=`echo $line | cut -d '=' -f 2-`

        if [ -z "$key" -o -z "$val" ] ; then
            log "WARN: in gen_cfg_for_zk_node(): key or val is empty: line=env:$line key=$key val=$val"
            continue
        fi

        sed -i -e "2 i $line" $zk_env || return 1
    done

    #Step-7: generate systemctl zookeeper.service
    local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
    local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
    local installation=$install_path/$zkdir

    local zk_srv=$zk_conf_dir/zookeeper.service.$node
    rm -f $zk_srv 
    cp -f $ZKSCRIPT_PDIR/systemd/zookeeper.service $zk_srv || return 1

    sed -i -e "s|^ExecStart=.*$|ExecStart=$installation/bin/zkServer.sh start|" $zk_srv || return 1
    sed -i -e "s|^ExecStop=.*$|ExecStop=$installation/bin/zkServer.sh stop|" $zk_srv || return 1
    sed -i -e "s|^User=.*$|User=$user|" $zk_srv || return 1
    sed -i -e "s|^Group=.*$|Group=$user|" $zk_srv || return 1

    log "INFO: Exit gen_cfg_for_zk_node(): Success"
    return 0
}

function gen_cfg_for_zk_nodes()
{
    local zk_conf_dir=$1

    log "INFO: Enter gen_cfg_for_zk_nodes(): zk_conf_dir=$zk_conf_dir"

    local zk_nodes=$zk_conf_dir/nodes

    #generate config files for common
    gen_cfg_for_zk_node "$zk_conf_dir" "common"
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit gen_cfg_for_zk_nodes(): failed to generate config files for 'common'"
        return 1
    fi

    #generate config files for specific nodes 
    local zoo_cfg_node_list=`mktemp --suffix=-stor-deploy.zoo_cfg_list`
    rm -f $zoo_cfg_node_list
    for node in `cat $zk_nodes` ; do
        gen_cfg_for_zk_node "$zk_conf_dir" "$node" "$zoo_cfg_node_list"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit gen_cfg_for_zk_nodes(): failed to generate config files for $node"
            return 1
        fi
    done

    #Notice that, in gen_cfg_for_zk_node() the zoo.cfg.$node generated is not complete, a list like this is missing:
    #          server.1=node1.localdomain:2888:3888
    #          server.2=node2.localdomain:2888:3888
    #          server.3=node3.localdomain:2888:3888
    #however, we have gathered the list in $zoo_cfg_node_list
    for zoo_cfg in `find $zk_conf_dir -name "zoo.cfg.*" -type f` ; do
        cat $zoo_cfg_node_list >> $zoo_cfg
    done

    rm -f $zoo_cfg_node_list

    log "INFO: Exit gen_cfg_for_zk_nodes(): Success"
    return 0
}

function dispatch_zk_configs()
{
    local zk_conf_dir=$1

    log "INFO: Enter dispatch_zk_configs(): zk_conf_dir=$zk_conf_dir"

    local zk_comm_cfg=$zk_conf_dir/common
    local zk_nodes=$zk_conf_dir/nodes

    local sshErr=`mktemp --suffix=-stor-deploy.dispatch_zk_cfg`

    for node in `cat $zk_nodes` ; do
        local node_cfg=$zk_comm_cfg
        [ -f $zk_conf_dir/$node ] && node_cfg=$zk_conf_dir/$node

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

        #find out the config files
        local zoo_cfg=$zk_conf_dir/zoo.cfg.common
        [ -f $zk_conf_dir/zoo.cfg.$node ] && zoo_cfg=$zk_conf_dir/zoo.cfg.$node

        local zkEnv_sh=$zk_conf_dir/zkEnv.sh.common
        [ -f $zk_conf_dir/zkEnv.sh.$node ] && zkEnv_sh=$zk_conf_dir/zkEnv.sh.$node

        local myid_file=$zk_conf_dir/myid.$node

        local zk_srv=$zk_conf_dir/zookeeper.service.common
        [ -f $zk_conf_dir/zookeeper.service.$node ] && zk_srv=$zk_conf_dir/zookeeper.service.$node

        log "INFO: in dispatch_zk_configs(): for $node: zoo_cfg=$zoo_cfg zkEnv_sh=$zkEnv_sh myid_file=$myid_file zk_srv=$zk_srv"

        if [ ! -s $zoo_cfg -o ! -s $zkEnv_sh -o ! -s $myid_file ] ; then
            log "ERROR: Exit dispatch_zk_configs(): file $zoo_cfg or $zkEnv_sh or $myid_file does not exist or is empty"
            return 1
        fi

        local SSH="ssh -p $ssh_port $user@$node"
        local SCP="scp -P $ssh_port"

        #copy the config files to zk servers respectively;
        $SCP $zoo_cfg $user@$node:$installation/conf/zoo.cfg
        $SCP $zkEnv_sh $user@$node:$installation/bin/zkEnv.sh
        $SCP $myid_file $user@$node:$dataDir/myid
        $SCP $zk_srv $user@$node:/usr/lib/systemd/system/zookeeper.service

        #check if the copy above succeeded or not;
        local remoteMD5=`mktemp --suffix=-stor-deploy.zk.remoteMD5`
        $SSH md5sum $installation/conf/zoo.cfg                         \
                    $installation/bin/zkEnv.sh $dataDir/myid           \
                    /usr/lib/systemd/system/zookeeper.service          \
                    2> $sshErr | cut -d ' ' -f 1 > $remoteMD5

        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit dispatch_zk_configs(): failed to get md5 of config files on $node. See $sshErr for details"
            return 1
        fi

        local localMD5=`mktemp --suffix=-stor-deploy.zk.localMD5`
        md5sum $zoo_cfg $zkEnv_sh $myid_file $zk_srv | cut -d ' ' -f 1 > $localMD5

        local md5Diff=`diff $remoteMD5 $localMD5`
        if [ -n "$md5Diff" ] ; then
            log "ERROR: Exit dispatch_zk_configs(): md5 of config files on $node is incorrect. See $sshErr, $remoteMD5 and $localMD5 for details"
            return 1
        fi
        rm -f $remoteMD5 $localMD5

        #reload and enable zookeeper service;
        log "INFO: in dispatch_zk_configs(): reload daemon and enable zookeeper: $SSH \"systemctl daemon-reload ; systemctl enable zookeeper\""
        $SSH "systemctl daemon-reload ; systemctl enable zookeeper" > $sshErr 2>&1
        sed -i -e '/Created symlink from/ d' $sshErr
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit dispatch_zk_configs(): failed to reload and enable zookeeper service on $node. See $sshErr for details"
            return 1
        fi

        $SSH "systemctl status zookeeper" > $sshErr 2>&1
        cat $sshErr | grep "Loaded: loaded" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit dispatch_zk_configs(): failed enable zookeeper service on $node. See $sshErr for details"
            return 1
        fi
    done

    rm -f $sshErr

    log "INFO: Exit dispatch_zk_configs(): Success"
    return 0
}

function start_zk_servers()
{
    local zk_conf_dir=$1

    log "INFO: Enter start_zk_servers(): zk_conf_dir=$zk_conf_dir"

    local zk_comm_cfg=$zk_conf_dir/common
    local zk_nodes=$zk_conf_dir/nodes

    local sshErr=`mktemp --suffix=-stor-deploy.dispatch_cfg`

    for node in `cat $zk_nodes` ; do
        local node_cfg=$zk_comm_cfg
        [ -f $zk_conf_dir/$node ] && node_cfg=$zk_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`

        local SSH="ssh -p $ssh_port $user@$node"

        log "INFO: in start_zk_servers(): start zookeeper service: $SSH systemctl start zookeeper"
        $SSH systemctl start zookeeper 2> $sshErr
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit start_zk_servers(): failed to start zookeeper on $node. See $sshErr for details"
            return 1
        fi
    done

    rm -f $sshErr

    log "INFO: Exit start_zk_servers(): Success"
    return 0
}

function verify_zk_status()
{
    local zk_conf_dir=$1

    log "INFO: Enter verify_zk_status(): zk_conf_dir=$zk_conf_dir"

    local zk_comm_cfg=$zk_conf_dir/common
    local zk_nodes=$zk_conf_dir/nodes

    local leader_found=""
    for node in `cat $zk_nodes` ; do
        local node_cfg=$zk_comm_cfg
        [ -f $zk_conf_dir/$node ] && node_cfg=$zk_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

        local installation=`basename $package`
        installation=`echo $installation | sed -e 's/.tar.gz$//' -e 's/.tgz$//'`
        installation=$install_path/$installation

        local SSH="ssh -p $ssh_port $user@$node"

        local succ=""
        for i in {1..5} ; do
            log "INFO: in verify_zk_status(): get mode of zookeeper service: $SSH $installation/bin/zkServer.sh status"
            local mode=`$SSH $installation/bin/zkServer.sh status 2> /dev/null | grep "Mode:" | cut -d ' ' -f 2`
            if [ "X$mode" = "Xleader" ] ; then
                log "INFO: in verify_zk_status(): mode of zookeeper on $node: $mode"
                leader_found="true"
                succ="true"
                break
            elif [ "X$mode" = "Xfollower" ] ; then
                log "INFO: in verify_zk_status(): mode of zookeeper on $node: $mode"
                succ="true"
                break
            fi
            sleep 2
        done

        if [ -z "$succ" ] ; then
            log "ERROR: Exit verify_zk_status(): failed to get mode of zookeeper service on $node"
            return 1
        fi
    done

    if [ -z "$leader_found" ] ; then
        log "ERROR: Exit verify_zk_status(): didn't found leader on all zookeeper nodes"
        return 1
    fi

    log "INFO: Exit verify_zk_status(): Success"
    return 0
}


function deploy_zk()
{
    local parsed_conf_dir=$1
    local from=$2
    local to=$3

    log "INFO: Enter deploy_zk(): parsed_conf_dir=$parsed_conf_dir from=$from to=$to"

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

    #Step-1: generate configurations for each zk node;
    if [ $from -le 1 -a $to -ge 1 ] ; then
        gen_cfg_for_zk_nodes $zk_conf_dir
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_zk(): failed to generate configuration files for each node"
            return 1
        fi
    fi

    for node in `cat $zk_nodes` ; do
        local node_cfg=$zk_comm_cfg
        [ -f $zk_conf_dir/$node ] && node_cfg=$zk_conf_dir/$node

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
            log "ERROR: Exit deploy_zk(): currently, only 'root' user is supported user=$user"
            return 1
        fi

        #we have checked the package in gen_cfg_for_zk_node() function: is name valid, if package exists ...
        local installation=`basename $package`
        installation=`echo $installation | sed -e 's/.tar.gz$//' -e 's/.tgz$//'`
        installation=$install_path/$installation

        #Step-2: check zk nodes (such as java environment)
        if [ $from -le 2 -a $to -ge 2 ] ; then
            log "INFO: in deploy_zk(): check zk node $node ..."
            check_zk_node "$node" "$user" "$ssh_port" "$java_home"
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit deploy_zk(): check_zk_node failed on $node"
                return 1
            fi
        fi

        #Step-3: stop zk process
        if [ $from -le 3 -a $to -ge 3 ] ; then
            log "INFO: in deploy_zk(): stop zk process on $node ..."
            stop_zk "$node" "$user" "$ssh_port"
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit deploy_zk(): stop_zk failed on $node"
                return 1
            fi
        fi

        #Step-4: uninstall existing installation
        if [ $from -le 4 -a $to -ge 4 ] ; then
            log "INFO: in deploy_zk(): uninstall zk on $node ..."
            uninstall_zk "$node" "$user" "$ssh_port" "$installation"
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit deploy_zk(): uninstall_zk failed on $node"
                return 1
            fi
        fi

        #Step-5: prepare (mount the disks, create the dirs)
        if [ $from -le 5 -a $to -ge 5 ] ; then
            log "INFO: in deploy_zk(): prepare zk node $node ..."
            prepare_zk_node "$node" "$user" "$ssh_port" "$dataDir" "$dataLogDir" "$logdir" "$pidfile" "$install_path" "$mounts" "$mount_opts" "$mkfs_cmd"
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit deploy_zk(): prepare_zk_node failed on $node"
                return 1
            fi
        fi
    done

    #Step-6: dispatch zk package to each node. Note that what's dispatched is the release-package, which doesn't
    #        contain our configurations. We will dispatch the configuation files later.
    if [ $from -le 6 -a $to -ge 6 ] ; then
        dispatch_zk_package $zk_conf_dir
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_zk(): failed to dispatch zookeeper package to some node"
            return 1
        fi
    fi

    #Step-7: dispatch configurations to each zk node;
    if [ $from -le 7 -a $to -ge 7 ] ; then
        dispatch_zk_configs $zk_conf_dir
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_zk(): failed to dispatch configuration files to each node"
            return 1
        fi
    fi

    #Step-8: start zookeeper servers on each zk node;
    if [ $from -le 8 -a $to -ge 8 ] ; then
        start_zk_servers $zk_conf_dir
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_zk(): failed to start zookeeper server on each node"
            return 1
        fi

        log "INFO: in deploy_zk(): sleep 5 seconds ..."
        sleep 5 
    fi

    #Step-9: verify if processes are running properly;
    if [ $from -le 9 -a $to -ge 9 ] ; then
        verify_zk_status $zk_conf_dir
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_zk(): failed to verify zookeeper status on each node"
            return 1
        fi
    fi

    log "INFO: Exit deploy_zk(): Success"
    return 0
}

fi
