#!/bin/bash

if [ -z "$hdfs_deploy_defined" ] ; then
hdfs_deploy_defined="true"

HDFSSCRIPT="${BASH_SOURCE-$0}"
HDFSSCRIPT_PDIR="$(dirname "${HDFSSCRIPT}")"
HDFSSCRIPT_PDIR="$(cd "${HDFSSCRIPT_PDIR}"; pwd)"

. $HDFSSCRIPT_PDIR/tools.sh

[ -z "$run_timestamp" ] && run_timestamp=`date +%Y%m%d%H%M%S`

function gen_cfg_for_hdfs_node()
{
    local hdfs_conf_dir=$1
    local node=$2

    log "INFO: Enter gen_cfg_for_hdfs_node(): hdfs_conf_dir=$hdfs_conf_dir node=$node"

    local node_cfg=$hdfs_conf_dir/$node
    local common_cfg=$hdfs_conf_dir/common

    #Step-1: check if specific config file exists for a node. if not exist, return;
    if [ "X$node" != "Xcommon" ] ; then
        if [ ! -f $node_cfg ] ; then
            log "INFO: Exit gen_cfg_for_hdfs_node(): $node does not have a specific config file"
            return 0
        else
            log "INFO: in gen_cfg_for_hdfs_node(): $node has a specific config file, generate hadoop conf files based on it"
        fi
    fi

    #Step-2: generate core-site.xml
    local core_site_file="$hdfs_conf_dir/core-site.xml.$node"
    rm -f $core_site_file

    echo '<?xml version="1.0" encoding="UTF-8"?>'                       >  $core_site_file
    echo '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>'  >> $core_site_file
    echo '<configuration>'                                              >> $core_site_file

    cat $node_cfg | grep "^core-site:" | while read line ; do
        line=`echo $line | sed -e 's/^core-site://'`

        echo $line | grep "=" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit gen_cfg_for_hdfs_node(): config core-site:$line is invalid"
            return 1
        fi

        local key=`echo $line | cut -d '=' -f 1`
        local val=`echo $line | cut -d '=' -f 2-`

        if [ -z "$key" -o -z "$val" ] ; then
            log "WARN: in gen_cfg_for_hdfs_node(): key or val is empty: line=core-site:$line key=$key val=$val"
            continue
        fi

        echo '    <property>'                 >> $core_site_file
        echo "        <name>$key</name>"      >> $core_site_file
        echo "        <value>$val</value>"    >> $core_site_file
        echo '    </property>'                >> $core_site_file
    done

    echo '</configuration>' >> $core_site_file

    #Step-3: generate hdfs-site.xml
    local hdfs_site_file="$hdfs_conf_dir/hdfs-site.xml.$node"
    rm -f $hdfs_site_file

    echo '<?xml version="1.0" encoding="UTF-8"?>'                       >  $hdfs_site_file
    echo '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>'  >> $hdfs_site_file
    echo '<configuration>'                                              >> $hdfs_site_file

    cat $node_cfg | grep "^hdfs-site:" | while read line ; do
        line=`echo $line | sed -e 's/^hdfs-site://'`

        echo $line | grep "=" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit gen_cfg_for_hdfs_node(): config hdfs-site:$line is invalid"
            return 1
        fi

        local key=`echo $line | cut -d '=' -f 1`
        local val=`echo $line | cut -d '=' -f 2-`

        if [ -z "$key" -o -z "$val" ] ; then
            log "WARN: in gen_cfg_for_hdfs_node(): key or val is empty: line=hdfs-site:$line key=$key val=$val"
            continue
        fi

        echo '    <property>'                 >> $hdfs_site_file
        echo "        <name>$key</name>"      >> $hdfs_site_file
        echo "        <value>$val</value>"    >> $hdfs_site_file
        echo '    </property>'                >> $hdfs_site_file
    done

    echo '</configuration>' >> $hdfs_site_file

    #Step-4: extract the package 
    local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

    if [ -z "$package" ] ; then
        log "ERROR: Exit gen_cfg_for_hdfs_node(): package must be configured, but it's empty"
        return 1
    fi

    if [[ "$package" =~ .tar.gz$ ]] || [[ "$package" =~ .tgz$ ]] ; then
        log "INFO: in gen_cfg_for_hdfs_node(): hadoop package name looks good. package=$package"
    else
        log "ERROR: Exit gen_cfg_for_hdfs_node(): hadoop package must be tar.gz or tgz package. package=$package"
        return 1
    fi

    if [ ! -s $package ] ; then
        log "ERROR: Exit gen_cfg_for_hdfs_node(): hadoop package doesn't exist or is empty. package=$package"
        return 1
    fi

    local hadoopdir=`basename $package`
    hadoopdir=`echo $hadoopdir | sed -e 's/.tar.gz$//' -e 's/.tgz$//'`

    local extract_dir=/tmp/hdfs-extract-$run_timestamp

    local src_hadoop_env=$extract_dir/$hadoopdir/etc/hadoop/hadoop-env.sh
    local src_slave_sh=$extract_dir/$hadoopdir/sbin/slaves.sh
    local src_daemon_sh=$extract_dir/$hadoopdir/sbin/hadoop-daemon.sh

    if [ ! -s $src_hadoop_env ] ; then 
        log "INFO: in gen_cfg_for_hdfs_node(): unzip hadoop package. package=$package ..."

        rm -fr $extract_dir || return 1
        mkdir -p $extract_dir || return 1

        tar -C $extract_dir -zxf $package  \
                                 $hadoopdir/etc/hadoop/hadoop-env.sh   \
                                 $hadoopdir/sbin/slaves.sh             \
                                 $hadoopdir/sbin/hadoop-daemon.sh || return 1

        if [ ! -s $src_hadoop_env -o ! -s $src_slave_sh -o ! -s $src_daemon_sh ] ; then 
            log "ERROR: Exit gen_cfg_for_hdfs_node(): failed to unzip hadoop package. package=$package extract_dir=$extract_dir"
            return 1
        fi
    else
        log "INFO: in gen_cfg_for_hdfs_node(): hadoop package has already been unzipped. package=$package"
    fi

    #Step-5: generate hadoop-env.sh
    local hadoop_env="$hdfs_conf_dir/hadoop-env.sh.$node"
    rm -f $hadoop_env
    cp -f $src_hadoop_env $hadoop_env || return 1

    cat $node_cfg | grep "^env:" | while read line ; do
        line=`echo $line | sed -e 's/^env://'`

        echo $line | grep "=" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit gen_cfg_for_hdfs_node(): config env:$line is invalid"
            return 1
        fi

        local key=`echo $line | cut -d '=' -f 1`
        local val=`echo $line | cut -d '=' -f 2-`

        if [ -z "$key" -o -z "$val" ] ; then
            log "WARN: in gen_cfg_for_hdfs_node(): key or val is empty: line=env:$line key=$key val=$val"
            continue
        fi

        sed -i -e "2 i $key=\"$val\"" $hadoop_env || return 1
    done

    #Step-6: add ssh port to some scripts. the scripts have been extracted in previous step.
    local slave_sh=$hdfs_conf_dir/slaves.sh.$node
    local daemon_sh=$hdfs_conf_dir/hadoop-daemon.sh.$node
    rm -f $slave_sh $daemon_sh

    cp -f $src_slave_sh $slave_sh || return 1
    cp -f $src_daemon_sh $daemon_sh || return 1

    local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`

    sed -i -e "2 i HADOOP_SSH_OPTS=\"-p $ssh_port\"" $slave_sh || return 1
    sed -i -e "s/rsync -a -e ssh/rsync -a -e \"ssh -p $ssh_port\"/" $daemon_sh || return 1

    #Step-7: generate systemctl service files 
    local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
    local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
    local installation=$install_path/$hadoopdir

    local hadoop_service=$hdfs_conf_dir/hadoop@.service.$node
    local hadoop_target=$hdfs_conf_dir/hadoop.target #it has no node specific configuration, so generate only one copy 
    rm -f $hadoop_service $hadoop_target

    cp -f $HDFSSCRIPT_PDIR/systemd/hadoop@.service $hadoop_service || return 1
    if [ ! -s $hadoop_target ] ; then
        cp -f $HDFSSCRIPT_PDIR/systemd/hadoop.target $hadoop_target || return 1
    fi

    sed -i -e "s|^ExecStart=.*$|ExecStart=$installation/sbin/hadoop-daemon.sh start %i|" $hadoop_service || return 1
    sed -i -e "s|^ExecStop=.*$|ExecStop=$installation/sbin/hadoop-daemon.sh stop %i|" $hadoop_service || return 1
    sed -i -e "s|^User=.*$|User=$user|" $hadoop_service || return 1
    sed -i -e "s|^Group=.*$|Group=$user|" $hadoop_service || return 1

    log "INFO: Exit gen_cfg_for_hdfs_node(): Success"
    return 0
}

function gen_cfg_for_hdfs_nodes()
{
    local hdfs_conf_dir=$1

    log "INFO: Enter gen_cfg_for_hdfs_nodes(): hdfs_conf_dir=$hdfs_conf_dir"

    local hdfs_nodes=$hdfs_conf_dir/nodes

    #generate config files for common
    gen_cfg_for_hdfs_node "$hdfs_conf_dir" "common"
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit gen_cfg_for_hdfs_nodes(): failed to generate config files for 'common'"
        return 1
    fi

    #generate config files for specific nodes 
    for node in `cat $hdfs_nodes` ; do
        gen_cfg_for_hdfs_node "$hdfs_conf_dir" "$node"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit gen_cfg_for_hdfs_nodes(): failed to generate config files for $node"
            return 1
        fi
    done

    log "INFO: Exit gen_cfg_for_hdfs_nodes(): Success"
    return 0
}

function check_zk_service_for_hdfs()
{
    local hdfs_conf_dir=$1

    log "INFO: Enter check_zk_service_for_hdfs(): hdfs_conf_dir=$hdfs_conf_dir"

    local hdfs_comm_cfg=$hdfs_conf_dir/common

    local zk_nodes=`grep "hdfs-site:ha.zookeeper.quorum" $hdfs_comm_cfg | cut -d '=' -f 2-`
    if [ -z "$zk_nodes" ] ; then
        log "INFO: Exit check_zk_service_for_hdfs(): ha.zookeeper.quorum is not configured."
        return 1
    fi

    local user=`grep "user=" $hdfs_comm_cfg | cut -d '=' -f 2-`
    local ssh_port=`grep "ssh_port=" $hdfs_comm_cfg | cut -d '=' -f 2-`
    local sshErr=`mktemp --suffix=-stor-deploy.check_zk`

    if [ "X$user" != "Xroot" ] ; then
        log "ERROR: Exit check_zk_service_for_hdfs(): currently, only 'root' user is supported user=$user"
        return 1
    fi

    while [ -n "$zk_nodes" ] ; do
        local zk_node=""
        echo $zk_nodes | grep "," > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            zk_node=`echo $zk_nodes | cut -d ',' -f 1`
            zk_nodes=`echo $zk_nodes | cut -d ',' -f 2-`

            zk_node=`echo $zk_node | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
            zk_nodes=`echo $zk_nodes | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
        else
            zk_node=$zk_nodes
            zk_nodes=""
        fi

        log "INFO: in check_zk_service_for_hdfs(): found a zookeeper node: $zk_node"

        echo "$zk_node" | grep ":" > /dev/null 2>&1
        local zk_host=""
        local zk_port=""
        if [ $? -eq 0 ] ; then
            zk_host=`echo $zk_node | cut -d ':' -f 1`
            zk_port=`echo $zk_node | cut -d ':' -f 2`
        else
            zk_host=$zk_node
            zk_port=2181  # 2181 is the default port for zookeeper
        fi

        #TODO: currently I din't find a way to check the zookeeper service based only on $zk_host and $zk_port.
        #      So I just check if QuorumPeerMain is running, this is not so good ...
        #Need to find a another way: such as telnet, which check if zk service is available at $zk_host and $zk_port,
        #      not relying on ssh.

        #And I cannot get the ssh user and port of $zk_host, just use user and ssh_port configured in $hdfs_comm_cfg
        #this may be wrong. (however currently we only support 'root' user)

        local SSH="ssh -p $ssh_port $user@$zk_host"
        local zk_pid=`$SSH jps 2> $sshErr | grep QuorumPeerMain | cut -d ' ' -f 1`
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit check_zk_service_for_hdfs(): failed to find running zookeepr on $zk_host. See $sshErr for details"
            return 1
        fi

        if [ -z "$zk_pid" ] ; then
            log "ERROR: Exit check_zk_service_for_hdfs(): there is no running zookeeper found on $zk_host"
            return 1
        fi

        log "INFO: in check_zk_service_for_hdfs(): zookeeper is running on $zk_host"
    done

    log "INFO: Exit check_zk_service_for_hdfs(): Success"
    return 0
}

function check_hdfs_node()
{
    local node=$1
    local user=$2
    local ssh_port=$3
    local java_home=$4

    log "INFO: Enter check_hdfs_node(): node=$node user=$user ssh_port=$ssh_port java_home=$java_home"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.check`

    $SSH $java_home/bin/java -version > $sshErr 2>&1
    cat $sshErr | grep "java version" > /dev/null 2>&1
    if [ $? -ne 0 ] ;  then  #didn't found "java version", so java is not available
        log "ERROR: Exit check_hdfs_node(): java is not availabe at $java_home on $node. See $sshErr for details"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit check_hdfs_node(): Success"
    return 0
} 

function cleanup_hdfs_node()
{
    local node=$1
    local user=$2
    local ssh_port=$3
    local installation=$4

    log "INFO: Enter cleanup_hdfs_node(): node=$node user=$user ssh_port=$ssh_port installation=$installation"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.hdfs_clean`

    local systemctl_cfgs="hadoop@journalnode.service hadoop@zkfc.service hadoop@namenode.service hadoop@datanode.service hadoop@.service hadoop.target"

    #Step-1: try to stop running hdfs processes if found.
    local succ=""
    local hdfs_pids=""
    for r in {1..10} ; do
        hdfs_pids=`$SSH jps 2> $sshErr | grep -e NameNode -e DataNode -e DFSZKFailoverController -e JournalNode | cut -d ' ' -f 1`
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit cleanup_hdfs_node(): failed to find hdfs processes on $node. See $sshErr for details"
            return 1
        fi

        if [ -z "$hdfs_pids" ] ; then
            $SSH "systemctl status $systemctl_cfgs" | grep "Active: active (running)" > /dev/null 2>&1
            if [ $? -ne 0 ] ; then # didn't find
                log "INFO: in cleanup_hdfs_node(): didn't find hdfs processes on $node"
                succ="true"
                break
            fi
        fi

        log "INFO: in cleanup_hdfs_node(): try to stop hdfs processes by systemctl: $SSH systemctl stop $systemctl_cfgs"
        $SSH "systemctl stop $systemctl_cfgs" 2> /dev/null
        sleep 5

        log "INFO: in cleanup_hdfs_node(): try to stop hdfs processes by kill"
        for hdfs_pid in $hdfs_pids ; do
            log "INFO: $SSH kill -9 $hdfs_pid"
            $SSH kill -9 $hdfs_pid 2> /dev/null
        done
        sleep 5
    done

    if [ "X$succ" != "Xtrue" ] ; then
        log "ERROR: Exit cleanup_hdfs_node(): failed to stop hdfs processes on $node hdfs_pids=$hdfs_pids"
        return 1
    fi

    #Step-2: try to remove the legacy hadoop installation;
    log "INFO: in cleanup_hdfs_node(): remove legacy hadoop installation if there is on $node"

    for systemctl_cfg in $systemctl_cfgs ; do
        log "INFO: in cleanup_hdfs_node(): disable hdfs: $SSH systemctl disable $systemctl_cfg"
        $SSH "systemctl disable $systemctl_cfg" 2> /dev/null
    done

    local backup=/tmp/hadoop-backup-$run_timestamp
    local systemctl_files=""
    for systemctl_cfg in $systemctl_cfgs ; do
        systemctl_files="$systemctl_files /usr/lib/systemd/system/$systemctl_cfg /etc/systemd/system/$systemctl_cfg"
    done
    #Yuanguo: fast test
    $SSH "mkdir -p $backup ; mv -f $installation $systemctl_files $backup" 2> /dev/null
    #$SSH "mkdir -p $backup ; mv -f $systemctl_files $backup" 2> /dev/null

    log "INFO: in cleanup_hdfs_node(): reload daemon: $SSH systemctl daemon-reload"
    $SSH systemctl daemon-reload 2> $sshErr 
    if [ -s "$sshErr" ] ; then
        log "ERROR: Exit cleanup_hdfs_node(): failed to reload daemon on $node. See $sshErr for details"
        return 1
    fi

    #Yuanguo: fast test
    $SSH ls $installation $systemctl_files > $sshErr 2>&1
    #$SSH ls $systemctl_files > $sshErr 2>&1
    sed -i -e '/No such file or directory/ d' $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit cleanup_hdfs_node(): ssh failed or we failed to remove legacy hadoop installation on $node. See $sshErr for details"
        return 1
    fi

    for systemctl_cfg in $systemctl_cfgs ; do
        log "INFO: in cleanup_hdfs_node(): $SSH systemctl status $systemctl_cfg"
        $SSH systemctl status $systemctl_cfg 2>&1 | grep -e "missing the instance name" -e "could not be found" -e "Loaded: not-found" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit cleanup_hdfs_node(): failed to check $systemctl_cfg on $node"
            return 1
        fi
    done

    rm -f $sshErr

    log "INFO: Exit cleanup_hdfs_node(): Success"
    return 0
}

function prepare_hdfs_node()
{
    #user must be 'root' for now, so we don't use sudo;
    local hdfs_conf_dir=$1
    local node=$2

    log "INFO: Enter prepare_hdfs_node(): hdfs_conf_dir=$hdfs_conf_dir node=$node"

    local hdfs_comm_cfg=$hdfs_conf_dir/common
    local node_cfg=$hdfs_comm_cfg
    [ -f $hdfs_conf_dir/$node ] && node_cfg=$hdfs_conf_dir/$node

    local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
    local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
    local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
    local mounts=`grep "mounts=" $node_cfg | cut -d '=' -f 2-`
    local mkfs_cmd=`grep "mkfs_cmd=" $node_cfg | cut -d '=' -f 2-`
    local mount_opts=`grep "mount_opts=" $node_cfg | cut -d '=' -f 2-`

    local pid_dir=`grep "env:HADOOP_PID_DIR=" $node_cfg | cut -d '=' -f 2-`
    local log_dir=`grep "env:HADOOP_LOG_DIR=" $node_cfg | cut -d '=' -f 2-`
    local sock_dir=`grep "hdfs-site:dfs.domain.socket.path=" $node_cfg | cut -d '=' -f 2-`
    local tmp_dir=`grep "core-site:hadoop.tmp.dir=" $node_cfg | cut -d '=' -f 2-`

    sock_dir=`dirname $sock_dir`

    log "INFO: Enter prepare_hdfs_node(): node=$node user=$user ssh_port=$ssh_port"
    log "INFO:       install_path=$install_path"
    log "INFO:       mounts=$mounts"
    log "INFO:       mkfs_cmd=$mkfs_cmd"
    log "INFO:       mount_opts=$mount_opts"
    log "INFO:       pid_dir=$pid_dir"
    log "INFO:       log_dir=$log_dir"
    log "INFO:       sock_dir=$sock_dir"
    log "INFO:       tmp_dir=$tmp_dir"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.prepare_hdfs_node`

    #Step-1: mount the disks
    if [ -n "$mounts" ] ; then
        if [ -z "$mount_opts" -o -z "$mkfs_cmd" ] ; then
            log "ERROR: Exit prepare_hdfs_node(): mount_opts and mkfs_cmd must be present when mounts is present"
            return 1
        fi

        do_mounts "$node" "$user" "$ssh_port" "$mounts" "$mount_opts" "$mkfs_cmd"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit prepare_hdfs_node(): do_mounts failed for $node"
            return 1
        fi
    fi

    #Step-2: create dirs for hadoop; 
    $SSH "mkdir -p $pid_dir $log_dir $sock_dir $tmp_dir $install_path" 2> $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit prepare_hdfs_node(): failed to create dirs for hadoop on $node. See $sshErr for details"
        return 1
    fi

    $SSH "rm -fr $tmp_dir/*" 2> $sshErr #don't rm $install_path/* (e.g. /usr/local/)
    if [ -s $sshErr ] ; then
        log "ERROR: Exit prepare_hdfs_node(): failed to rm legacy data of hadoop on $node. See $sshErr for details"
        return 1
    fi

    local nn_nodes_file=$hdfs_conf_dir/name-nodes
    local dn_nodes_file=$hdfs_conf_dir/data-nodes
    local jn_nodes_file=$hdfs_conf_dir/journal-nodes

    if [ ! -s $nn_nodes_file -o ! -s $dn_nodes_file -o ! -s $jn_nodes_file ] ; then
        log "ERROR: Exit prepare_hdfs_node(): file $nn_nodes_file, $dn_nodes_file or $jn_nodes_file does not exist or is empty"
        return 1
    fi

    grep -w $node $nn_nodes_file > /dev/null
    if [ $? -eq 0 ] ; then  # $node is a name node
        local nn_name_dir=`grep "hdfs-site:dfs.namenode.name.dir=" $node_cfg | cut -d '=' -f 2-`
        local nn_name_dir=`echo $nn_name_dir | sed -e 's/,/ /g'`

        $SSH "mkdir -p $nn_name_dir" 2> $sshErr
        if [ -s $sshErr ] ; then
            log "ERROR: Exit prepare_hdfs_node(): failed to create dirs for name-node $node. See $sshErr for details"
            return 1
        fi

        for adir in $nn_name_dir ; do
            $SSH rm -fr $adir/* 2> $sshErr
            if [ -s $sshErr ] ; then
                log "ERROR: Exit prepare_hdfs_node(): failed to rm legacy data for name-node $node. See $sshErr for details"
                return 1
            fi
        done
    fi

    grep -w $node $dn_nodes_file > /dev/null
    if [ $? -eq 0 ] ; then  # $node is a data node
        local dn_data_dir=`grep "hdfs-site:dfs.datanode.data.dir=" $node_cfg | cut -d '=' -f 2-`
        local dn_data_dir=`echo $dn_data_dir | sed -e 's/,/ /g'`

        $SSH "mkdir -p $dn_data_dir" 2> $sshErr
        if [ -s $sshErr ] ; then
            log "ERROR: Exit prepare_hdfs_node(): failed to create dirs for data-node $node. See $sshErr for details"
            return 1
        fi

        for adir in $dn_data_dir ; do
            $SSH rm -fr $adir/* 2> $sshErr
            if [ -s $sshErr ] ; then
                log "ERROR: Exit prepare_hdfs_node(): failed to rm legacy data for data-node $node. See $sshErr for details"
                return 1
            fi
        done
    fi

    grep -w $node $jn_nodes_file > /dev/null
    if [ $? -eq 0 ] ; then  # $node is a journal node
        local jn_edits_dir=`grep "hdfs-site:dfs.journalnode.edits.dir=" $node_cfg | cut -d '=' -f 2-`
        local jn_edits_dir=`echo $jn_edits_dir | sed -e 's/,/ /g'`

        $SSH "mkdir -p $jn_edits_dir" 2> $sshErr
        if [ -s $sshErr ] ; then
            log "ERROR: Exit prepare_hdfs_node(): failed to create dirs for journal-node $node. See $sshErr for details"
            return 1
        fi

        for adir in $jn_edits_dir ; do
            $SSH rm -fr $adir/* 2> $sshErr
            if [ -s $sshErr ] ; then
                log "ERROR: Exit prepare_hdfs_node(): failed to rm legacy data for journal-node $node. See $sshErr for details"
                return 1
            fi
        done
    fi
    
    rm -f $sshErr

    log "INFO: Exit prepare_hdfs_node(): Success"
    return 0
}

function dispatch_hdfs_package()
{
    local hdfs_conf_dir=$1

    log "INFO: Enter dispatch_hdfs_package(): hdfs_conf_dir=$hdfs_conf_dir"

    local hdfs_comm_cfg=$hdfs_conf_dir/common
    local hdfs_nodes=$hdfs_conf_dir/nodes

    local package=""
    local src_md5=""
    local base_name=""

    for node in `cat $hdfs_nodes` ; do
        local node_cfg=$hdfs_comm_cfg
        [ -f $hdfs_conf_dir/$node ] && node_cfg=$hdfs_conf_dir/$node

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
                log "ERROR: Exit dispatch_hdfs_package(): package for $node is different from others. thePack=$thePack package=$package"
                return 1
            fi
        fi

        log "INFO: in dispatch_hdfs_package(): start background task: scp -P $ssh_port $package $user@$node:$install_path"
        scp -P $ssh_port $package $user@$node:$install_path &
    done

    wait

    local sshErr=`mktemp --suffix=-stor-deploy.hdfs_dispatch`
    for node in `cat $hdfs_nodes` ; do
        local node_cfg=$hdfs_comm_cfg
        [ -f $hdfs_conf_dir/$node ] && node_cfg=$hdfs_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

        local SSH="ssh -p $ssh_port $user@$node"

        local dst_md5=`$SSH md5sum $install_path/$base_name 2> $sshErr | cut -d ' ' -f 1`
        if [ -s $sshErr ] ; then
            log "ERROR: Exit dispatch_hdfs_package(): failed to get md5sum of $install_path/$base_name on $node. See $sshErr for details"
            return 1
        fi

        if [ "X$dst_md5" != "X$src_md5" ] ; then
            log "ERROR: Exit dispatch_hdfs_package(): md5sum of $install_path/$base_name on $node is incorrect. src_md5=$src_md5 dst_md5=$dst_md5"
            return 1
        fi

        log "INFO: in dispatch_hdfs_package(): start background task: $SSH tar zxf $install_path/$base_name -C $install_path"
        $SSH tar zxf $install_path/$base_name -C $install_path 2> $sshErr &
        if [ -s $sshErr ] ; then
            log "ERROR: Exit dispatch_hdfs_package(): failed to extract $install_path/$base_name on $node. See $sshErr for details"
            return 1
        fi
    done

    wait



    rm -f $sshErr

    log "INFO: Exit dispatch_hdfs_package(): Success"
    return 0
}

function dispatch_hdfs_configs()
{
    local hdfs_conf_dir=$1

    log "INFO: Enter dispatch_hdfs_configs(): hdfs_conf_dir=$hdfs_conf_dir"

    local hdfs_comm_cfg=$hdfs_conf_dir/common
    local hdfs_nodes=$hdfs_conf_dir/nodes
    local hdfs_name_nodes=$hdfs_conf_dir/name-nodes
    local hdfs_data_nodes=$hdfs_conf_dir/data-nodes
    local hdfs_journal_nodes=$hdfs_conf_dir/journal-nodes

    local sshErr=`mktemp --suffix=-stor-deploy.dispatch_hdfs_cfg`

    for node in `cat $hdfs_nodes` ; do
        local node_cfg=$hdfs_comm_cfg
        [ -f $hdfs_conf_dir/$node ] && node_cfg=$hdfs_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

        log "INFO: in dispatch_hdfs_configs(): node=$node node_cfg=$node_cfg"
        log "INFO:        user=$user"
        log "INFO:        ssh_port=$ssh_port"
        log "INFO:        install_path=$install_path"
        log "INFO:        package=$package"

        local installation=`basename $package`
        installation=`echo $installation | sed -e 's/.tar.gz$//' -e 's/.tgz$//'`
        installation=$install_path/$installation


        #find out the config files
        local hadoop_env_sh=$hdfs_conf_dir/hadoop-env.sh.common
        [ -f $hdfs_conf_dir/hadoop-env.sh.$node ] && hadoop_env_sh=$hdfs_conf_dir/hadoop-env.sh.$node

        local core_site_xml=$hdfs_conf_dir/core-site.xml.common
        [ -f $hdfs_conf_dir/core-site.xml.$node ] && core_site_xml=$hdfs_conf_dir/core-site.xml.$node

        local hdfs_site_xml=$hdfs_conf_dir/hdfs-site.xml.common
        [ -f $hdfs_conf_dir/hdfs-site.xml.$node ] && hdfs_site_xml=$hdfs_conf_dir/hdfs-site.xml.$node

        local had_daemon_sh=$hdfs_conf_dir/hadoop-daemon.sh.common
        [ -f $hdfs_conf_dir/hadoop-daemon.sh.$node ] && had_daemon_sh=$hdfs_conf_dir/hadoop-daemon.sh.$node

        local slaves_sh=$hdfs_conf_dir/slaves.sh.common
        [ -f $hdfs_conf_dir/slaves.sh.$node ] && slaves_sh=$hdfs_conf_dir/slaves.sh.$node

        local hadoop_target=$hdfs_conf_dir/hadoop.target
        local hadoop_service=$hdfs_conf_dir/hadoop@.service.common
        [ -f $hdfs_conf_dir/hadoop@.service.$node ] && hadoop_service=$hdfs_conf_dir/hadoop@.service.$node

        log "INFO: in dispatch_hdfs_configs(): for $node: "
        log "INFO: in dispatch_hdfs_configs():         hadoop_env_sh=$hadoop_env_sh"
        log "INFO: in dispatch_hdfs_configs():         core_site_xml=$core_site_xml"
        log "INFO: in dispatch_hdfs_configs():         hdfs_site_xml=$hdfs_site_xml"
        log "INFO: in dispatch_hdfs_configs():         had_daemon_sh=$had_daemon_sh"
        log "INFO: in dispatch_hdfs_configs():         slaves_sh=$slaves_sh"
        log "INFO: in dispatch_hdfs_configs():         hadoop_target=$hadoop_target"
        log "INFO: in dispatch_hdfs_configs():         hadoop_service=$hadoop_service"

        if [ ! -s $hadoop_env_sh -o ! -s $core_site_xml -o ! -s $hdfs_site_xml -o ! -s $slaves_sh \
                   -o ! -s $had_daemon_sh -o ! -s $hadoop_target -o ! -s $hadoop_service ] ; then
            log "ERROR: Exit dispatch_hdfs_configs(): some config file does not exist or is empty"
            return 1
        fi

        local SSH="ssh -p $ssh_port $user@$node"
        local SCP="scp -P $ssh_port"

        #copy the config files to hdfs servers respectively;
        $SCP $hadoop_env_sh  $user@$node:$installation/etc/hadoop/hadoop-env.sh
        $SCP $core_site_xml  $user@$node:$installation/etc/hadoop/core-site.xml
        $SCP $hdfs_site_xml  $user@$node:$installation/etc/hadoop/hdfs-site.xml
        $SCP $had_daemon_sh  $user@$node:$installation/sbin/hadoop-daemon.sh
        $SCP $slaves_sh      $user@$node:$installation/sbin/slaves.sh
        $SCP $hadoop_target  $user@$node:/usr/lib/systemd/system/hadoop.target
        $SCP $hadoop_service $user@$node:/usr/lib/systemd/system/hadoop@.service

        #check if the copy above succeeded or not;
        local remoteMD5=`mktemp --suffix=-stor-deploy.hdfs.remoteMD5`
        $SSH md5sum                                            \
                  $installation/etc/hadoop/hadoop-env.sh       \
                  $installation/etc/hadoop/core-site.xml       \
                  $installation/etc/hadoop/hdfs-site.xml       \
                  $installation/sbin/hadoop-daemon.sh          \
                  $installation/sbin/slaves.sh                 \
                  /usr/lib/systemd/system/hadoop.target        \
                  /usr/lib/systemd/system/hadoop@.service      \
                  2> $sshErr | cut -d ' ' -f 1 > $remoteMD5

        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit dispatch_hdfs_configs(): failed to get md5 of config files on $node. See $sshErr for details"
            return 1
        fi

        local localMD5=`mktemp --suffix=-stor-deploy.hdfs.localMD5`
        md5sum                      \
                $hadoop_env_sh      \
                $core_site_xml      \
                $hdfs_site_xml      \
                $had_daemon_sh      \
                $slaves_sh          \
                $hadoop_target      \
                $hadoop_service     \
                | cut -d ' ' -f 1 > $localMD5

        local md5Diff=`diff $remoteMD5 $localMD5`
        if [ -n "$md5Diff" ] ; then
            log "ERROR: Exit dispatch_hdfs_configs(): md5 of config files on $node is incorrect. See $sshErr, $remoteMD5 and $localMD5 for details"
            return 1
        fi
        rm -f $remoteMD5 $localMD5

        #reload and enable hadoop daemons;
        local enable_services=""
        local num=0
        grep -w $node $hdfs_name_nodes > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            enable_services="$enable_services hadoop@namenode.service hadoop@zkfc.service"
            num=`expr $num + 2`
        fi

        grep -w $node $hdfs_data_nodes > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            enable_services="$enable_services hadoop@datanode.service"
            num=`expr $num + 1`
        fi

        grep -w $node $hdfs_journal_nodes > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            enable_services="$enable_services hadoop@journalnode.service"
            num=`expr $num + 1`
        fi

        log "INFO: in dispatch_hdfs_configs(): $SSH systemctl daemon-reload ; systemctl enable $enable_services"
        $SSH "systemctl daemon-reload ; systemctl enable $enable_services" > $sshErr 2>&1
        sed -i -e '/Created symlink from/ d' $sshErr
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit dispatch_hdfs_configs(): failed to reload and enable hadoop service on $node. enable_services=$enable_services. See $sshErr for details"
            return 1
        fi

        log "INFO: in dispatch_hdfs_configs(): $SSH systemctl status $enable_services"
        $SSH "systemctl status $enable_services" > $sshErr 2>&1
        local n=`cat $sshErr | grep "Loaded: loaded" | wc -l`
        if [ $n -ne $num ] ; then
            log "ERROR: Exit dispatch_hdfs_configs(): failed enable hadoop service on $node. enable_services=$enable_services. See $sshErr for details"
            return 1
        fi
    done

    rm -f $sshErr

    log "INFO: Exit dispatch_hdfs_configs(): Success"
    return 0
}

function start_hdfs_daemons()
{
    local hdfs_conf_dir=$1

    log "INFO: Enter start_hdfs_daemons(): hdfs_conf_dir=$hdfs_conf_dir"

    local hdfs_comm_cfg=$hdfs_conf_dir/common
    local hdfs_name_nodes=$hdfs_conf_dir/name-nodes
    local hdfs_data_nodes=$hdfs_conf_dir/data-nodes
    local hdfs_journal_nodes=$hdfs_conf_dir/journal-nodes

    local sshErr=`mktemp --suffix=-stor-deploy.start_hdfs`

    #Step-1: start all journal nodes
    for node in `cat $hdfs_journal_nodes` ; do
        local node_cfg=$hdfs_comm_cfg
        [ -f $hdfs_conf_dir/$node ] && node_cfg=$hdfs_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`

        local SSH="ssh -p $ssh_port $user@$node"

        log "INFO: $SSH systemctl start hadoop@journalnode"
        $SSH "systemctl start hadoop@journalnode" 2> $sshErr
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit start_hdfs_daemons(): error occurred when starting journal-node on $node. See $sshErr for details"
            return 1
        fi
    done

    #select one namenode as master and the other as standby
    local master_nn_ssh=""
    local master_nn_install=""

    local standby_nn_ssh=""
    local standby_nn_install=""

    for node in `cat $hdfs_name_nodes` ; do
        local node_cfg=$hdfs_comm_cfg
        [ -f $hdfs_conf_dir/$node ] && node_cfg=$hdfs_conf_dir/$node

        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`
        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`

        local installation=`basename $package`
        installation=`echo $installation | sed -e 's/.tar.gz$//' -e 's/.tgz$//'`
        installation=$install_path/$installation

        local SSH="ssh -p $ssh_port $user@$node"

        if [ -z "$master_nn_ssh" ] ; then
            master_nn_ssh="$SSH"
            master_nn_install="$installation"
        elif [ -z "$standby_nn_ssh" ] ; then
            standby_nn_ssh="$SSH"
            standby_nn_install="$installation"
        else
            log "ERROR: Exit start_hdfs_daemons(): more than two namenodes configured. Currently only two supported"
            return 1
        fi
    done

    if [ -z "$master_nn_ssh" -o -z "$standby_nn_ssh" ] ; then
        log "ERROR: Exit start_hdfs_daemons(): less than two namenodes configured. master_nn_ssh=$master_nn_ssh standby_nn_ssh=$standby_nn_ssh"
        return 1
    fi

    #Steps 2-8 are for namenodes;

    #Step-2: format namedb on master namenode, and start master name-node;
    log "INFO: $master_nn_ssh $master_nn_install/bin/hdfs namenode -format -force"
    $master_nn_ssh "$master_nn_install/bin/hdfs namenode -format -force" 2> $sshErr
    grep "INFO util.ExitUtil: Exiting with status 0" $sshErr > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit start_hdfs_daemons(): failed to format namedb on master name-node. See $sshErr for details"
        return 1
    fi

    #Step-3: start master name-node
    log "INFO: $master_nn_ssh systemctl start hadoop@namenode"
    $master_nn_ssh "systemctl start hadoop@namenode" 2> $sshErr
    if [ -s "$sshErr" ] ; then
        log "ERROR: Exit start_hdfs_daemons(): error occurred when starting master name-node. See $sshErr for details"
        return 1
    fi

    #Step-4: bootstrap standby name-node
    log "INFO: $standby_nn_ssh $standby_nn_install/bin/hdfs namenode -bootstrapStandby -force"
    $standby_nn_ssh "$standby_nn_install/bin/hdfs namenode -bootstrapStandby -force" 2> $sshErr
    grep "util.ExitUtil: Exiting with status 0" $sshErr > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit start_hdfs_daemons(): error occurred when bootstrap standby name-node. See $sshErr for details"
        return 1
    fi

    #Step-5: formatZK on master name-node
    log "INFO: $master_nn_ssh $master_nn_install/bin/hdfs zkfc -formatZK -force"
    $master_nn_ssh "$master_nn_install/bin/hdfs zkfc -formatZK -force" 2> $sshErr
    grep "Successfully created.*in ZK" $sshErr > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit start_hdfs_daemons(): error occurred when formatZK on master name-node. See $sshErr for details"
        return 1
    fi

    #Step-6: start zkfc on master name-node
    log "INFO: $master_nn_ssh systemctl start hadoop@zkfc"
    $master_nn_ssh "systemctl start hadoop@zkfc" 2> $sshErr
    if [ -s "$sshErr" ] ; then
        log "ERROR: Exit start_hdfs_daemons(): error occurred when starting zkfc on master name-node. See $sshErr for details"
        return 1
    fi
    
    #Step-7: start standby name-node
    log "INFO: $standby_nn_ssh systemctl start hadoop@namenode"
    $standby_nn_ssh "systemctl start hadoop@namenode" 2> $sshErr
    if [ -s "$sshErr" ] ; then
        log "ERROR: Exit start_hdfs_daemons(): error occurred when starting standby name-node. See $sshErr for details"
        return 1
    fi

    #Step-8: start zkfc on standby name-node
    log "INFO: $standby_nn_ssh systemctl start hadoop@zkfc"
    $standby_nn_ssh "systemctl start hadoop@zkfc" 2> $sshErr
    if [ -s "$sshErr" ] ; then
        log "ERROR: Exit start_hdfs_daemons(): error occurred when starting zkfc on standby name-node. See $sshErr for details"
        return 1
    fi

    #Step-9: start all data-nodes
    for node in `cat $hdfs_data_nodes` ; do
        local node_cfg=$hdfs_comm_cfg
        [ -f $hdfs_conf_dir/$node ] && node_cfg=$hdfs_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`

        local SSH="ssh -p $ssh_port $user@$node"

        log "INFO: $SSH systemctl start hadoop@datanode"
        $SSH "systemctl start hadoop@datanode" 2> $sshErr
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit start_hdfs_daemons(): error occurred when starting datanode on $node. See $sshErr for details"
            return 1
        fi
    done

    rm -f $sshErr

    log "INFO: Exit start_hdfs_daemons(): Success"
    return 0
}

function check_hdfs_status()
{
    local hdfs_conf_dir=$1
    log "INFO: Enter check_hdfs_status(): hdfs_conf_dir=$hdfs_conf_dir"

    local hdfs_comm_cfg=$hdfs_conf_dir/common
    local hdfs_nodes=$hdfs_conf_dir/nodes
    local hdfs_name_nodes=$hdfs_conf_dir/name-nodes
    local hdfs_data_nodes=$hdfs_conf_dir/data-nodes
    local hdfs_journal_nodes=$hdfs_conf_dir/journal-nodes

    local sshErr=`mktemp --suffix=-stor-deploy.chk_hdfs`
    local java_processes=`mktemp --suffix=-stor-deploy.chk_hdfs_jps`

    for node in `cat $hdfs_nodes` ; do
        local node_cfg=$hdfs_comm_cfg
        [ -f $hdfs_conf_dir/$node ] && node_cfg=$hdfs_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

        log "INFO: in check_hdfs_status(): node=$node node_cfg=$node_cfg"
        log "INFO:        user=$user"
        log "INFO:        ssh_port=$ssh_port"
        log "INFO:        install_path=$install_path"
        log "INFO:        package=$package"

        local installation=`basename $package`
        installation=`echo $installation | sed -e 's/.tar.gz$//' -e 's/.tgz$//'`
        installation=$install_path/$installation

        local SSH="ssh -p $ssh_port $user@$node"

        $SSH jps > $java_processes 2>&1

        #Step-1: check name nodes
        grep -w $node $hdfs_name_nodes > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            grep -w "NameNode" $java_processes > /dev/null 2>&1
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit check_hdfs_status(): NameNode was not found on $node. See $java_processes for details"
                return 1
            fi
            grep -w "DFSZKFailoverController" $java_processes > /dev/null 2>&1
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit check_hdfs_status(): DFSZKFailoverController was not found on $node. See $java_processes for details"
                return 1
            fi

            #$node is namenode, we run haadmin on it, checking status of all namenodes.
            local nns=`grep "hdfs-site:dfs.ha.namenodes" $node_cfg | cut -d '=' -f 2- | sed -e 's/,/ /g'`
            if [ -z "$nns" ] ; then
                log "ERROR: Exit check_hdfs_status(): hdfs-site:dfs.ha.namenodes.{nameservice} was not configured corretly. See $node_cfg for details"
                return 1
            fi

            local active_found=""
            local standby_found=""
            for nn in $nns ; do
                log "INFO: $SSH $installation/bin/hdfs haadmin -getServiceState $nn"
                $SSH "$installation/bin/hdfs haadmin -getServiceState $nn" > $sshErr 2>&1
                grep -i -w "Active" $sshErr > /dev/null 2>&1
                if [ $? -eq 0 ] ; then
                    active_found="true"
                else
                    grep -i -w "Standby" $sshErr > /dev/null 2>&1
                    if [ $? -eq 0 ] ; then
                        standby_found="true"
                    else
                        log "ERROR: Exit check_hdfs_status(): failed to get service state of $nn. See $sshErr for details"
                        return 1
                    fi
                fi

                log "INFO: $SSH $installation/bin/hdfs haadmin -checkHealth $nn && echo yes"
                $SSH "$installation/bin/hdfs haadmin -checkHealth $nn && echo yes" | grep -w "yes" > /dev/null 2>&1
                if [ $? -ne 0 ] ; then
                    log "ERROR: Exit check_hdfs_status(): $nn is not healthy."
                    return 1
                fi
            done

            if [ -z "$standby_found" -o -z "$active_found" ] ; then
                log "ERROR: Exit check_hdfs_status(): didn't find active namenode or standby namenode. See $sshErr for details"
                return 1
            fi
        fi

        #Step-2: check journal nodes
        grep -w $node $hdfs_journal_nodes > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            grep -w "JournalNode" $java_processes > /dev/null 2>&1
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit check_hdfs_status(): JournalNode was not found on $node. See $java_processes for details"
                return 1
            fi
        fi

        #Step-3: check data nodes
        grep -w $node $hdfs_data_nodes > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            grep -w "DataNode" $java_processes > /dev/null 2>&1
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit check_hdfs_status(): DataNode was not found on $node. See $java_processes for details"
                return 1
            fi
        fi
    done

    rm -f $sshErr $java_processes
    
    log "INFO: Exit check_hdfs_status(): Success"
    return 0
}

function deploy_hdfs()
{
    local parsed_conf_dir=$1
    local stop_after=$2
    local zk_included=$3

    log "INFO: Enter deploy_hdfs(): parsed_conf_dir=$parsed_conf_dir stop_after=$stop_after zk_included=$zk_included"

    local hdfs_conf_dir=$parsed_conf_dir/hdfs
    local hdfs_comm_cfg=$hdfs_conf_dir/common
    local hdfs_nodes=$hdfs_conf_dir/nodes

    if [ ! -d $hdfs_conf_dir ] ; then
        log "ERROR: Exit deploy_hdfs(): dir $hdfs_conf_dir does not exist"
        return 1
    fi

    if [ ! -f $hdfs_comm_cfg -o ! -f $hdfs_nodes ] ; then
        log "ERROR: Exit deploy_hdfs(): file $hdfs_comm_cfg or $hdfs_nodes does not exist"
        return 1
    fi

    #Step-1: generate configurations for each hdfs node;
    gen_cfg_for_hdfs_nodes $hdfs_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_hdfs(): failed to generate configuration files for each node"
        return 1
    fi

    if [ "X$stop_after" = "Xparse" ] ; then
        log "INFO: Exit deploy_hdfs(): stop early because stop_after=$stop_after"
        return 0
    fi

    #Step-2: if zookeeper is not included in this deployment, then zookeeper service must be available.
    if [ "X$zk_included" != "Xtrue" ] ; then
        check_zk_service_for_hdfs $hdfs_conf_dir
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_hdfs(): zookeeper service is not availabe"
            return 1
        fi
    fi

    #Step-3: check hdfs nodes (such as java environment), clean up hdfs nodes, and prepare (mount the disks, create the dirs)
    for node in `cat $hdfs_nodes` ; do
        local node_cfg=$hdfs_comm_cfg
        [ -f $hdfs_conf_dir/$node ] && node_cfg=$hdfs_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local java_home=`grep "env:JAVA_HOME=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

        log "INFO: in deploy_hdfs(): node=$node node_cfg=$node_cfg"
        log "INFO:        user=$user"
        log "INFO:        ssh_port=$ssh_port"
        log "INFO:        java_home=$java_home"
        log "INFO:        install_path=$install_path"
        log "INFO:        package=$package"


        if [ "X$user" != "Xroot" ] ; then
            log "ERROR: Exit deploy_hdfs(): currently, only 'root' user is supported user=$user"
            return 1
        fi

        #we have checked the package in gen_cfg_for_hdfs_node() function: is name valid, if package exists ...
        local installation=`basename $package`
        installation=`echo $installation | sed -e 's/.tar.gz$//' -e 's/.tgz$//'`
        installation=$install_path/$installation

        #check the node, such as java environment
        log "INFO: in deploy_hdfs(): check hdfs node $node ..."
        check_hdfs_node "$node" "$user" "$ssh_port" "$java_home"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_hdfs(): check_hdfs_node failed on $node"
            return 1
        fi

        [ "X$stop_after" = "Xcheck" ] && continue

        #clean up hdfs node
        log "INFO: in deploy_hdfs(): clean up hdfs node $node ..."
        cleanup_hdfs_node "$node" "$user" "$ssh_port" "$installation"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_hdfs(): cleanup_hdfs_node failed on $node"
            return 1
        fi

        [ "X$stop_after" = "Xclean" ] && continue

        #prepare environment
        log "INFO: in deploy_hdfs(): prepare hdfs node $node ..."
        prepare_hdfs_node "$hdfs_conf_dir" "$node"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_hdfs(): prepare_hdfs_node failed on $node"
            return 1
        fi
    done

    if [ "X$stop_after" = "Xcheck" -o "X$stop_after" = "Xclean" -o "X$stop_after" = "Xprepare" ] ; then
        log "INFO: Exit deploy_hdfs(): stop early because stop_after=$stop_after"
        return 0
    fi

    #Step-4: dispatch hdfs package to each node. Note that what's dispatched is the release-package, which doesn't
    #        contain our configurations. We will dispatch the configuation files later.
    #Yuanguo: fast test
    dispatch_hdfs_package $hdfs_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_hdfs(): failed to dispatch hadoop package to some node"
        return 1
    fi

    #Step-5: dispatch configurations to each hdfs node;
    dispatch_hdfs_configs $hdfs_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_hdfs(): failed to dispatch configuration files to each node"
        return 1
    fi

    if [ "X$stop_after" = "Xinstall" ] ; then
        log "INFO: Exit deploy_hdfs(): stop early because stop_after=$stop_after"
        return 0
    fi

    #Step-6: start hdfs daemons on each hdfs node;
    start_hdfs_daemons $hdfs_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_hdfs(): failed to start hdfs daemons on some node"
        return 1
    fi

    log "INFO: in deploy_hdfs(): sleep 10 seconds before checking hdfs status ..."
    sleep 10 

    #Step-7: check hdfs status;
    check_hdfs_status $hdfs_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_hdfs(): failed to check hdfs status"
        return 1
    fi

    log "INFO: Exit deploy_hdfs(): Success"
    return 0
}

fi
