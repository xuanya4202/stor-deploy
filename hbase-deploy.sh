#!/bin/bash

if [ -z "$hbase_deploy_defined" ] ; then
hbase_deploy_defined="true"

HBASESCRIPT="${BASH_SOURCE-$0}"
HBASESCRIPT_PDIR="$(dirname "${HBASESCRIPT}")"
HBASESCRIPT_PDIR="$(cd "${HBASESCRIPT_PDIR}"; pwd)"

. $HBASESCRIPT_PDIR/tools.sh

function gen_cfg_for_hbase_node()
{
    local hbase_conf_dir=$1
    local node=$2

    log "INFO: Enter gen_cfg_for_hbase_node(): hbase_conf_dir=$hbase_conf_dir node=$node"

    local node_cfg=$hbase_conf_dir/$node
    local common_cfg=$hbase_conf_dir/common

    #Step-1: check if specific config file exists for a node. if not exist, return;
    if [ "X$node" != "Xcommon" ] ; then
        if [ ! -f $node_cfg ] ; then
            log "INFO: Exit gen_cfg_for_hbase_node(): $node does not have a specific config file"
            return 0
        else
            log "INFO: in gen_cfg_for_hbase_node(): $node has a specific config file, generate hbase conf files based on it"
        fi
    fi

    #Step-2: generate hbase-site.xml
    local hbase_site_file="$hbase_conf_dir/hbase-site.xml.$node"
    rm -f $hbase_site_file

    echo '<?xml version="1.0"?>'                                        >  $hbase_site_file
    echo '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>'  >> $hbase_site_file
    echo '<configuration>'                                              >> $hbase_site_file

    cat $node_cfg | grep "^hbase-site:" | while read line ; do
        line=`echo $line | sed -e 's/^hbase-site://'`

        echo $line | grep "=" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit gen_cfg_for_hbase_node(): config hbase-site:$line is invalid"
            return 1
        fi

        local key=`echo $line | cut -d '=' -f 1`
        local val=`echo $line | cut -d '=' -f 2-`

        if [ -z "$key" -o -z "$val" ] ; then
            log "WARN: in gen_cfg_for_hbase_node(): key or val is empty: line=hbase-site:$line key=$key val=$val"
            continue
        fi

        echo '    <property>'                 >> $hbase_site_file
        echo "        <name>$key</name>"      >> $hbase_site_file
        echo "        <value>$val</value>"    >> $hbase_site_file
        echo '    </property>'                >> $hbase_site_file
    done

    echo '</configuration>' >> $hbase_site_file

    #Step-3: extract the package 
    local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

    if [ -z "$package" ] ; then
        log "ERROR: Exit gen_cfg_for_hbase_node(): package must be configured, but it's empty"
        return 1
    fi

    if [[ "$package" =~ .tar.gz$ ]] || [[ "$package" =~ .tgz$ ]] ; then
        log "INFO: in gen_cfg_for_hbase_node(): hbase package name looks good. package=$package"
    else
        log "ERROR: Exit gen_cfg_for_hbase_node(): hbase package must be tar.gz or tgz package. package=$package"
        return 1
    fi

    if [ ! -s $package ] ; then
        log "ERROR: Exit gen_cfg_for_hbase_node(): hbase package doesn't exist or is empty. package=$package"
        return 1
    fi

    local hbasedir=`basename $package`
    hbasedir=`echo $hbasedir | sed -e 's/.tar.gz$//' -e 's/.tgz$//' -e 's/-bin//'`

    local extract_dir=/tmp/hbase-extract-$run_timestamp

    local src_hbase_env=$extract_dir/$hbasedir/conf/hbase-env.sh
    local src_regionservers_sh=$extract_dir/$hbasedir/bin/regionservers.sh
    local src_master_backup_sh=$extract_dir/$hbasedir/bin/master-backup.sh
    local src_zookeepers_sh=$extract_dir/$hbasedir/bin/zookeepers.sh
    local src_hbase_cleanup_sh=$extract_dir/$hbasedir/bin/hbase-cleanup.sh

    if [ ! -s $src_hbase_env ] ; then 
        log "INFO: in gen_cfg_for_hbase_node(): unzip hbase package. package=$package ..."

        rm -fr $extract_dir || return 1
        mkdir -p $extract_dir || return 1

        tar -C $extract_dir -zxf $package                        \
                                 $hbasedir/conf/hbase-env.sh     \
                                 $hbasedir/bin/regionservers.sh  \
                                 $hbasedir/bin/master-backup.sh  \
                                 $hbasedir/bin/zookeepers.sh     \
                                 $hbasedir/bin/hbase-cleanup.sh || return 1

        if [ ! -s $src_hbase_env -o ! -s $src_regionservers_sh -o ! -s $src_master_backup_sh -o \
             ! -s $src_zookeepers_sh -o ! -s $src_hbase_cleanup_sh ] ; then 
            log "ERROR: Exit gen_cfg_for_hbase_node(): failed to unzip hbase package. package=$package extract_dir=$extract_dir"
            return 1
        fi
    else
        log "INFO: in gen_cfg_for_hbase_node(): hbase package has already been unzipped. package=$package"
    fi

    #Step-4: generate hbase-env.sh
    local hbase_env="$hbase_conf_dir/hbase-env.sh.$node"
    rm -f $hbase_env
    cp -f $src_hbase_env $hbase_env || return 1

    cat $node_cfg | grep "^env:" | while read line ; do
        line=`echo $line | sed -e 's/^env://'`

        echo $line | grep "=" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit gen_cfg_for_hbase_node(): config env:$line is invalid"
            return 1
        fi

        local key=`echo $line | cut -d '=' -f 1`
        local val=`echo $line | cut -d '=' -f 2-`

        if [ -z "$key" -o -z "$val" ] ; then
            log "WARN: in gen_cfg_for_hbase_node(): key or val is empty: line=env:$line key=$key val=$val"
            continue
        fi

        sed -i -e "/^export[[:space:]][[:space:]]*$key/ d" $hbase_env || return 1
        sed -i -e "2 i export $key=\"$val\"" $hbase_env || return 1
    done

    #Step-5: add ssh port to some scripts. the scripts have been extracted in previous step.
    local regionservers_sh=$hbase_conf_dir/regionservers.sh.$node
    local master_backup_sh=$hbase_conf_dir/master-backup.sh.$node
    local zookeepers_sh=$hbase_conf_dir/zookeepers.sh.$node
    local hbase_cleanup_sh=$hbase_conf_dir/hbase-cleanup.sh.$node

    rm -f $regionservers_sh $master_backup_sh $zookeepers_sh $hbase_cleanup_sh 

    cp -f  $src_regionservers_sh  $regionservers_sh || return 1
    cp -f  $src_master_backup_sh  $master_backup_sh || return 1
    cp -f  $src_zookeepers_sh     $zookeepers_sh    || return 1
    cp -f  $src_hbase_cleanup_sh  $hbase_cleanup_sh || return 1

    local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`

    sed -i -e "2 i HBASE_SSH_OPTS=\"-p $ssh_port\"" $regionservers_sh $master_backup_sh $zookeepers_sh $hbase_cleanup_sh || return 1

    #Step-6: generate systemctl service files 
    local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
    local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
    local installation=$install_path/$hbasedir

    local hbase_service=$hbase_conf_dir/hbase@.service.$node
    local hbase_target=$hbase_conf_dir/hbase.target #it has no node specific configuration, so generate only one copy 
    rm -f $hbase_service $hbase_target

    cp -f $HBASESCRIPT_PDIR/systemd/hbase@.service $hbase_service || return 1
    if [ ! -s $hbase_target ] ; then
        cp -f $HBASESCRIPT_PDIR/systemd/hbase.target $hbase_target || return 1
    fi

    sed -i -e "s|^ExecStart=.*$|ExecStart=$installation/bin/hbase-daemon.sh start %i|" $hbase_service || return 1
    sed -i -e "s|^ExecStop=.*$|ExecStop=$installation/bin/hbase-daemon.sh stop %i|" $hbase_service || return 1
    sed -i -e "s|^User=.*$|User=$user|" $hbase_service || return 1
    sed -i -e "s|^Group=.*$|Group=$user|" $hbase_service || return 1

    log "INFO: Exit gen_cfg_for_hbase_node(): Success"
    return 0
}

function gen_cfg_for_hbase_nodes()
{
    local hbase_conf_dir=$1

    log "INFO: Enter gen_cfg_for_hbase_nodes(): hbase_conf_dir=$hbase_conf_dir"

    local hbase_nodes=$hbase_conf_dir/nodes

    #generate config files for common
    gen_cfg_for_hbase_node "$hbase_conf_dir" "common"
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit gen_cfg_for_hbase_nodes(): failed to generate config files for 'common'"
        return 1
    fi

    #generate config files for specific nodes 
    for node in `cat $hbase_nodes` ; do
        gen_cfg_for_hbase_node "$hbase_conf_dir" "$node"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit gen_cfg_for_hbase_nodes(): failed to generate config files for $node"
            return 1
        fi
    done

    log "INFO: Exit gen_cfg_for_hbase_nodes(): Success"
    return 0
}

function check_zk_service_for_hbase()
{
    local hbase_conf_dir=$1

    log "INFO: Enter check_zk_service_for_hbase(): hbase_conf_dir=$hbase_conf_dir"

    local hbase_comm_cfg=$hbase_conf_dir/common

    local zk_nodes=`grep "hbase-site:hbase.zookeeper.quorum" $hbase_comm_cfg | cut -d '=' -f 2-`
    if [ -z "$zk_nodes" ] ; then
        log "INFO: Exit check_zk_service_for_hbase(): hbase-site:hbase.zookeeper.quorum is not configured."
        return 1
    fi

    local user=`grep "user=" $hbase_comm_cfg | cut -d '=' -f 2-`
    local ssh_port=`grep "ssh_port=" $hbase_comm_cfg | cut -d '=' -f 2-`
    local sshErr=`mktemp --suffix=-stor-deploy.check_zk`

    if [ "X$user" != "Xroot" ] ; then
        log "ERROR: Exit check_zk_service_for_hbase(): currently, only 'root' user is supported user=$user"
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

        log "INFO: in check_zk_service_for_hbase(): found a zookeeper node: $zk_node"

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

        #And I cannot get the ssh user and port of $zk_host, just use user and ssh_port configured in $hbase_comm_cfg
        #this may be wrong. (however currently we only support 'root' user)

        local SSH="ssh -p $ssh_port $user@$zk_host"
        local zk_pid=`$SSH jps 2> $sshErr | grep QuorumPeerMain | cut -d ' ' -f 1`
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit check_zk_service_for_hbase(): failed to find running zookeepr on $zk_host. See $sshErr for details"
            return 1
        fi

        if [ -z "$zk_pid" ] ; then
            log "ERROR: Exit check_zk_service_for_hbase(): there is no running zookeeper found on $zk_host"
            return 1
        fi

        log "INFO: in check_zk_service_for_hbase(): zookeeper is running on $zk_host"
    done

    log "INFO: Exit check_zk_service_for_hbase(): Success"
    return 0
}

function check_hbase_node()
{
    local node=$1
    local user=$2
    local ssh_port=$3
    local java_home=$4

    log "INFO: Enter check_hbase_node(): node=$node user=$user ssh_port=$ssh_port java_home=$java_home"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.check`

    $SSH $java_home/bin/java -version > $sshErr 2>&1
    cat $sshErr | grep "java version" > /dev/null 2>&1
    if [ $? -ne 0 ] ;  then  #didn't found "java version", so java is not available
        log "ERROR: Exit check_hbase_node(): java is not availabe at $java_home on $node. See $sshErr for details"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit check_hbase_node(): Success"
    return 0
}

function stop_hbase()
{
    local node=$1
    local user=$2
    local ssh_port=$3

    log "INFO: Enter stop_hbase(): node=$node user=$user ssh_port=$ssh_port"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.hbase_stop`

    local systemctl_cfgs="hbase@master.service hbase@regionserver.service hbase@thrift2.service hbase@.service hbase.target"

    #Step-1: try to stop running hbase processes if found.
    local succ=""
    local hbase_pids=""
    for r in {1..10} ; do
        hbase_pids=`$SSH jps 2> $sshErr | grep -e ThriftServer -e HRegionServer -e HMaster | cut -d ' ' -f 1`
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit stop_hbase(): failed to find hbase processes on $node. See $sshErr for details"
            return 1
        fi

        if [ -z "$hbase_pids" ] ; then
            $SSH "systemctl status $systemctl_cfgs" | grep "Active: active (running)" > /dev/null 2>&1
            if [ $? -ne 0 ] ; then # didn't find
                log "INFO: in stop_hbase(): didn't find hbase processes on $node"
                succ="true"
                break
            fi
        fi

        log "INFO: in stop_hbase(): try to stop hbase processes by systemctl"

        local stop_cmds=""
        for systemctl_cfg in $systemctl_cfgs ; do
            stop_cmds="systemctl stop $systemctl_cfg ; $stop_cmds"
        done

        log "INFO: $SSH $stop_cmds"
        $SSH "$stop_cmds" 2> /dev/null
        sleep 5

        log "INFO: in stop_hbase(): try to stop hbase processes by kill"
        for hbase_pid in $hbase_pids ; do
            log "INFO: $SSH kill -9 $hbase_pid"
            $SSH kill -9 $hbase_pid 2> /dev/null
        done
        sleep 5
    done

    if [ "X$succ" != "Xtrue" ] ; then
        log "ERROR: Exit stop_hbase(): failed to stop hbase processes on $node hbase_pids=$hbase_pids"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit stop_hbase(): Success"
    return 0
}

function uninstall_hbase()
{
    local node=$1
    local user=$2
    local ssh_port=$3
    local installation=$4

    log "INFO: Enter uninstall_hbase(): node=$node user=$user ssh_port=$ssh_port installation=$installation"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.hbase_uninstall`

    local systemctl_cfgs="hbase@master.service hbase@regionserver.service hbase@thrift2.service hbase@.service hbase.target"

    #Step-1: try to remove the legacy hbase installation;
    log "INFO: in uninstall_hbase(): remove legacy hbase installation if there is on $node"

    local disable_cmds=""
    for systemctl_cfg in $systemctl_cfgs ; do
        disable_cmds="systemctl disable $systemctl_cfg ; $disable_cmds"
    done

    log "INFO: $SSH $disable_cmds"
    $SSH "$disable_cmds" 2> /dev/null

    local backup=/tmp/hbase-backup-$run_timestamp
    local systemctl_files=""
    for systemctl_cfg in $systemctl_cfgs ; do
        systemctl_files="$systemctl_files /usr/lib/systemd/system/$systemctl_cfg /etc/systemd/system/$systemctl_cfg"
    done
    $SSH "mkdir -p $backup ; mv -f $installation $systemctl_files $backup" 2> /dev/null

    log "INFO: in uninstall_hbase(): reload daemon: $SSH systemctl daemon-reload"
    $SSH systemctl daemon-reload 2> $sshErr 
    if [ -s "$sshErr" ] ; then
        log "ERROR: Exit uninstall_hbase(): failed to reload daemon on $node. See $sshErr for details"
        return 1
    fi

    $SSH ls $installation $systemctl_files > $sshErr 2>&1
    sed -i -e '/No such file or directory/ d' $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit uninstall_hbase(): ssh failed or we failed to remove legacy hbase installation on $node. See $sshErr for details"
        return 1
    fi

    for systemctl_cfg in $systemctl_cfgs ; do
        log "INFO: in uninstall_hbase(): $SSH systemctl status $systemctl_cfg"
        $SSH systemctl status $systemctl_cfg 2>&1 | grep -e "missing the instance name" -e "could not be found" -e "Loaded: not-found" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit uninstall_hbase(): failed to check $systemctl_cfg on $node"
            return 1
        fi
    done

    rm -f $sshErr

    log "INFO: Exit uninstall_hbase(): Success"
    return 0
}

function prepare_hbase_node()
{
    #user must be 'root' for now, so we don't use sudo;
    local hbase_conf_dir=$1
    local node=$2

    log "INFO: Enter prepare_hbase_node(): hbase_conf_dir=$hbase_conf_dir node=$node"

    local hbase_comm_cfg=$hbase_conf_dir/common
    local node_cfg=$hbase_comm_cfg
    [ -f $hbase_conf_dir/$node ] && node_cfg=$hbase_conf_dir/$node

    local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
    local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
    local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`

    local pid_dir=`grep "env:HBASE_PID_DIR=" $node_cfg | cut -d '=' -f 2-`
    local log_dir=`grep "env:HBASE_LOG_DIR=" $node_cfg | cut -d '=' -f 2-`

    log "INFO: Enter prepare_hbase_node(): node=$node user=$user ssh_port=$ssh_port"
    log "INFO:       install_path=$install_path"
    log "INFO:       pid_dir=$pid_dir"
    log "INFO:       log_dir=$log_dir"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.prepare_hbase_node`

    $SSH "mkdir -p $pid_dir $log_dir $install_path" 2> $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit prepare_hbase_node(): failed to create dirs for hbase on $node. See $sshErr for details"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit prepare_hbase_node(): Success"
    return 0
}

function dispatch_hbase_package()
{
    local hbase_conf_dir=$1

    log "INFO: Enter dispatch_hbase_package(): hbase_conf_dir=$hbase_conf_dir"

    local hbase_comm_cfg=$hbase_conf_dir/common
    local hbase_nodes=$hbase_conf_dir/nodes

    local package=""
    local src_md5=""
    local base_name=""

    for node in `cat $hbase_nodes` ; do
        local node_cfg=$hbase_comm_cfg
        [ -f $hbase_conf_dir/$node ] && node_cfg=$hbase_conf_dir/$node

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
                log "ERROR: Exit dispatch_hbase_package(): package for $node is different from others. thePack=$thePack package=$package"
                return 1
            fi
        fi

        log "INFO: in dispatch_hbase_package(): start background task: scp -P $ssh_port $package $user@$node:$install_path"
        scp -P $ssh_port $package $user@$node:$install_path &
    done

    wait

    local sshErr=`mktemp --suffix=-stor-deploy.hbase_dispatch`
    for node in `cat $hbase_nodes` ; do
        local node_cfg=$hbase_comm_cfg
        [ -f $hbase_conf_dir/$node ] && node_cfg=$hbase_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

        local SSH="ssh -p $ssh_port $user@$node"

        local dst_md5=`$SSH md5sum $install_path/$base_name 2> $sshErr | cut -d ' ' -f 1`
        if [ -s $sshErr ] ; then
            log "ERROR: Exit dispatch_hbase_package(): failed to get md5sum of $install_path/$base_name on $node. See $sshErr for details"
            return 1
        fi

        if [ "X$dst_md5" != "X$src_md5" ] ; then
            log "ERROR: Exit dispatch_hbase_package(): md5sum of $install_path/$base_name on $node is incorrect. src_md5=$src_md5 dst_md5=$dst_md5"
            return 1
        fi

        log "INFO: in dispatch_hbase_package(): start background task: $SSH tar zxf $install_path/$base_name -C $install_path"
        $SSH tar zxf $install_path/$base_name -C $install_path 2> $sshErr &
        if [ -s $sshErr ] ; then
            log "ERROR: Exit dispatch_hbase_package(): failed to extract $install_path/$base_name on $node. See $sshErr for details"
            return 1
        fi
    done

    wait

    rm -f $sshErr

    log "INFO: Exit dispatch_hbase_package(): Success"
    return 0
}

function dispatch_hbase_configs()
{
    local hbase_conf_dir=$1

    log "INFO: Enter dispatch_hbase_configs(): hbase_conf_dir=$hbase_conf_dir"

    local hbase_comm_cfg=$hbase_conf_dir/common
    local hbase_nodes=$hbase_conf_dir/nodes
    local hbase_master_nodes=$hbase_conf_dir/master-nodes
    local hbase_region_nodes=$hbase_conf_dir/region-nodes

    local sshErr=`mktemp --suffix=-stor-deploy.dispatch_hbase_cfg`

    for node in `cat $hbase_nodes` ; do
        local node_cfg=$hbase_comm_cfg
        [ -f $hbase_conf_dir/$node ] && node_cfg=$hbase_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

        log "INFO: in dispatch_hbase_configs(): node=$node node_cfg=$node_cfg"
        log "INFO:        user=$user"
        log "INFO:        ssh_port=$ssh_port"
        log "INFO:        install_path=$install_path"
        log "INFO:        package=$package"

        local installation=`basename $package`
        installation=`echo $installation | sed -e 's/.tar.gz$//' -e 's/.tgz$//' -e 's/-bin//'`
        installation=$install_path/$installation

        #find out the config files
        local hbase_site_xml=$hbase_conf_dir/hbase-site.xml.common
        [ -f $hbase_conf_dir/hbase-site.xml.$node ] && hbase_site_xml=$hbase_conf_dir/hbase-site.xml.$node

        local hbase_env_sh=$hbase_conf_dir/hbase-env.sh.common
        [ -f $hbase_conf_dir/hbase-env.sh.$node ] && hbase_env_sh=$hbase_conf_dir/hbase-env.sh.$node

        # we need to copy core-site.xml.common and hdfs-site.xml.common to ${HBASE_HOME}/conf/core-site.xml and
        # ${HBASE_HOME}/conf/hdfs-site.xml
        local hdfs_core_site=$hbase_conf_dir/../hdfs/core-site.xml.common
        local hdfs_hdfs_site=$hbase_conf_dir/../hdfs/hdfs-site.xml.common
        if [ ! -s $hdfs_core_site -o ! -s $hdfs_hdfs_site ] ; then
            log "ERROR: Exit dispatch_hbase_configs(): didn't found hdfs core-site.xml.common and/or hdfs-site.xml.common at $hbase_conf_dir/../hdfs"
            return 1
        fi

        #local regionservers=$hbase_conf_dir/region-nodes

        local regionservers_sh=$hbase_conf_dir/regionservers.sh.common
        [ -f $hbase_conf_dir/regionservers.sh.$node ] && regionservers_sh=$hbase_conf_dir/regionservers.sh.$node

        local master_backup_sh=$hbase_conf_dir/master-backup.sh.common
        [ -f $hbase_conf_dir/master-backup.sh.$node ] && master_backup_sh=$hbase_conf_dir/master-backup.sh.$node

        local zookeepers_sh=$hbase_conf_dir/zookeepers.sh.common
        [ -f $hbase_conf_dir/zookeepers.sh.$node ] && zookeepers_sh=$hbase_conf_dir/zookeepers.sh.$node

        local hbase_cleanup_sh=$hbase_conf_dir/hbase-cleanup.sh.common
        [ -f $hbase_conf_dir/hbase-cleanup.sh.$node ] && hbase_cleanup_sh=$hbase_conf_dir/hbase-cleanup.sh.$node

        local hbase_target=$hbase_conf_dir/hbase.target
        local hbase_service=$hbase_conf_dir/hbase@.service.common
        [ -f $hbase_conf_dir/hbase@.service.$node ] && hbase_service=$hbase_conf_dir/hbase@.service.$node


        log "INFO: in dispatch_hbase_configs(): for $node: "
        log "INFO: in dispatch_hbase_configs():         hbase_site_xml=$hbase_site_xml"
        log "INFO: in dispatch_hbase_configs():         hbase_env_sh=$hbase_env_sh"
        log "INFO: in dispatch_hbase_configs():         hdfs_core_site=$hdfs_core_site"
        log "INFO: in dispatch_hbase_configs():         hdfs_hdfs_site=$hdfs_hdfs_site"
        #log "INFO: in dispatch_hbase_configs():         regionservers=$regionservers"
        log "INFO: in dispatch_hbase_configs():         regionservers_sh=$regionservers_sh"
        log "INFO: in dispatch_hbase_configs():         master_backup_sh=$master_backup_sh"
        log "INFO: in dispatch_hbase_configs():         zookeepers_sh=$zookeepers_sh"
        log "INFO: in dispatch_hbase_configs():         hbase_cleanup_sh=$hbase_cleanup_sh"
        log "INFO: in dispatch_hbase_configs():         hbase_target=$hbase_target"
        log "INFO: in dispatch_hbase_configs():         hbase_service=$hbase_service"

        if [ ! -s $hbase_site_xml -o ! -s $hbase_env_sh -o ! -s $regionservers_sh -o ! -s $master_backup_sh -o \
             ! -s $zookeepers_sh -o ! -s $hbase_cleanup_sh -o ! -s $hbase_target -o ! -s $hbase_service ] ; then
            log "ERROR: Exit dispatch_hbase_configs(): some config file does not exist or is empty"
            return 1
        fi

        local SSH="ssh -p $ssh_port $user@$node"
        local SCP="scp -P $ssh_port"

        #copy the config files to hbase servers respectively;

        $SCP $hbase_site_xml    $user@$node:$installation/conf/hbase-site.xml
        $SCP $hbase_env_sh      $user@$node:$installation/conf/hbase-env.sh
        $SCP $hdfs_core_site    $user@$node:$installation/conf/core-site.xml
        $SCP $hdfs_hdfs_site    $user@$node:$installation/conf/hdfs-site.xml
        #$SCP $regionservers     $user@$node:$installation/conf/regionservers
        $SCP $regionservers_sh  $user@$node:$installation/bin/regionservers.sh
        $SCP $master_backup_sh  $user@$node:$installation/bin/master-backup.sh
        $SCP $zookeepers_sh     $user@$node:$installation/bin/zookeepers.sh
        $SCP $hbase_cleanup_sh  $user@$node:$installation/bin/hbase-cleanup.sh
        $SCP $hbase_target      $user@$node:/usr/lib/systemd/system/hbase.target
        $SCP $hbase_service     $user@$node:/usr/lib/systemd/system/hbase@.service

        #check if the copy above succeeded or not;
        local remoteMD5=`mktemp --suffix=-stor-deploy.hbase.remoteMD5`
        $SSH md5sum                                            \
                  $installation/conf/hbase-site.xml            \
                  $installation/conf/hbase-env.sh              \
                  $installation/conf/core-site.xml             \
                  $installation/conf/hdfs-site.xml             \
                  $installation/bin/regionservers.sh           \
                  $installation/bin/master-backup.sh           \
                  $installation/bin/zookeepers.sh              \
                  $installation/bin/hbase-cleanup.sh           \
                  /usr/lib/systemd/system/hbase.target         \
                  /usr/lib/systemd/system/hbase@.service       \
                  2> $sshErr | cut -d ' ' -f 1 > $remoteMD5

        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit dispatch_hbase_configs(): failed to get md5 of config files on $node. See $sshErr for details"
            return 1
        fi

        local localMD5=`mktemp --suffix=-stor-deploy.hbase.localMD5`
        md5sum                       \
                $hbase_site_xml      \
                $hbase_env_sh        \
                $hdfs_core_site      \
                $hdfs_hdfs_site      \
                $regionservers_sh    \
                $master_backup_sh    \
                $zookeepers_sh       \
                $hbase_cleanup_sh    \
                $hbase_target        \
                $hbase_service       \
                | cut -d ' ' -f 1 > $localMD5

        local md5Diff=`diff $remoteMD5 $localMD5`
        if [ -n "$md5Diff" ] ; then
            log "ERROR: Exit dispatch_hbase_configs(): md5 of config files on $node is incorrect. See $sshErr, $remoteMD5 and $localMD5 for details"
            return 1
        fi
        rm -f $remoteMD5 $localMD5

        #reload and enable hbase daemons;
        local enable_services="hbase@thrift2.service"
        local num=1
        grep -w $node $hbase_master_nodes > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            enable_services="$enable_services hbase@master.service"
            num=`expr $num + 1`
        fi

        grep -w $node $hbase_region_nodes > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            enable_services="$enable_services hbase@regionserver.service"
            num=`expr $num + 1`
        fi

        log "INFO: in dispatch_hbase_configs(): $SSH systemctl daemon-reload ; systemctl enable $enable_services"
        $SSH "systemctl daemon-reload ; systemctl enable $enable_services" > $sshErr 2>&1
        sed -i -e '/Created symlink from/ d' $sshErr
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit dispatch_hbase_configs(): failed to reload and enable hbase service on $node. enable_services=$enable_services. See $sshErr for details"
            return 1
        fi

        log "INFO: in dispatch_hbase_configs(): $SSH systemctl status $enable_services"
        $SSH "systemctl status $enable_services" > $sshErr 2>&1
        local n=`cat $sshErr | grep "Loaded: loaded" | wc -l`
        if [ $n -ne $num ] ; then
            log "ERROR: Exit dispatch_hbase_configs(): failed enable hbase service on $node. enable_services=$enable_services. See $sshErr for details"
            return 1
        fi
    done

    rm -f $sshErr

    log "INFO: Exit dispatch_hbase_configs(): Success"
    return 0
}

function start_hbase_daemons()
{
    local hbase_conf_dir=$1

    log "INFO: Enter start_hbase_daemons(): hbase_conf_dir=$hbase_conf_dir"

    local hbase_comm_cfg=$hbase_conf_dir/common
    local hbase_nodes=$hbase_conf_dir/nodes
    local hbase_master_nodes=$hbase_conf_dir/master-nodes
    local hbase_region_nodes=$hbase_conf_dir/region-nodes

    local sshErr=`mktemp --suffix=-stor-deploy.start_hbase`

    #Step-1: start all master nodes
    for node in `cat $hbase_master_nodes` ; do
        local node_cfg=$hbase_comm_cfg
        [ -f $hbase_conf_dir/$node ] && node_cfg=$hbase_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`

        local SSH="ssh -p $ssh_port $user@$node"

        log "INFO: $SSH systemctl start hbase@master"
        $SSH "systemctl start hbase@master" 2> $sshErr
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit start_hbase_daemons(): error occurred when starting hbase master on $node. See $sshErr for details"
            return 1
        fi
    done

    #Step-2: start all regionservers 
    for node in `cat $hbase_region_nodes` ; do
        local node_cfg=$hbase_comm_cfg
        [ -f $hbase_conf_dir/$node ] && node_cfg=$hbase_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`

        local SSH="ssh -p $ssh_port $user@$node"

        log "INFO: $SSH systemctl start hbase@regionserver"
        $SSH "systemctl start hbase@regionserver" 2> $sshErr
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit start_hbase_daemons(): error occurred when starting hbase regionserver on $node. See $sshErr for details"
            return 1
        fi
    done

    #Step-3: start all thrift2 servers 
    for node in `cat $hbase_nodes` ; do
        local node_cfg=$hbase_comm_cfg
        [ -f $hbase_conf_dir/$node ] && node_cfg=$hbase_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`

        local SSH="ssh -p $ssh_port $user@$node"

        log "INFO: $SSH systemctl start hbase@thrift2"
        $SSH "systemctl start hbase@thrift2" 2> $sshErr
        if [ -s "$sshErr" ] ; then
            log "ERROR: Exit start_hbase_daemons(): error occurred when starting hbase thrift2 on $node. See $sshErr for details"
            return 1
        fi
    done

    rm -f $sshErr

    log "INFO: Exit start_hbase_daemons(): Success"
    return 0
}

function check_hbase_status()
{
    local hbase_conf_dir=$1
    log "INFO: Enter check_hbase_status(): hbase_conf_dir=$hbase_conf_dir"

    local hbase_comm_cfg=$hbase_conf_dir/common
    local hbase_nodes=$hbase_conf_dir/nodes
    local hbase_master_nodes=$hbase_conf_dir/master-nodes
    local hbase_region_nodes=$hbase_conf_dir/region-nodes

    local sshErr=`mktemp --suffix=-stor-deploy.chk_hbase`
    local java_processes=`mktemp --suffix=-stor-deploy.chk_hbase_jps`

    for node in `cat $hbase_nodes` ; do
        local node_cfg=$hbase_comm_cfg
        [ -f $hbase_conf_dir/$node ] && node_cfg=$hbase_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

        log "INFO: in check_hbase_status(): node=$node node_cfg=$node_cfg"
        log "INFO:        user=$user"
        log "INFO:        ssh_port=$ssh_port"
        log "INFO:        install_path=$install_path"
        log "INFO:        package=$package"

        local installation=`basename $package`
        installation=`echo $installation | sed -e 's/.tar.gz$//' -e 's/.tgz$//' -e 's/-bin//'`
        installation=$install_path/$installation

        local SSH="ssh -p $ssh_port $user@$node"

        $SSH jps > $java_processes 2>&1

        #Step-1: check master
        grep -w $node $hbase_master_nodes > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            grep -w "HMaster" $java_processes > /dev/null 2>&1
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit check_hbase_status(): HMaster was not found on $node. See $java_processes for details"
                return 1
            fi
        fi

        #Step-2: check regionserver
        grep -w $node $hbase_region_nodes > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            grep -w "HRegionServer" $java_processes > /dev/null 2>&1
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit check_hbase_status(): HRegionServer was not found on $node. See $java_processes for details"
                return 1
            fi
        fi

        #Step-3: check thrift2
        grep -w "ThriftServer" $java_processes > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit check_hbase_status(): ThriftServer was not found on $node. See $java_processes for details"
            return 1
        fi
    done

    rm -f $sshErr $java_processes
    
    log "INFO: Exit check_hbase_status(): Success"
    return 0
}

function deploy_hbase()
{
    local parsed_conf_dir=$1
    local from=$2
    local to=$3
    local zk_included=$4

    log "INFO: Enter deploy_hbase(): parsed_conf_dir=$parsed_conf_dir from=$from to=$to zk_included=$zk_included"

    local hbase_conf_dir=$parsed_conf_dir/hbase
    local hbase_comm_cfg=$hbase_conf_dir/common
    local hbase_nodes=$hbase_conf_dir/nodes

    if [ ! -d $hbase_conf_dir ] ; then
        log "ERROR: Exit deploy_hbase(): dir $hbase_conf_dir does not exist"
        return 1
    fi

    if [ ! -f $hbase_comm_cfg -o ! -f $hbase_nodes ] ; then
        log "ERROR: Exit deploy_hbase(): file $hbase_comm_cfg or $hbase_nodes does not exist"
        return 1
    fi

    #Step-1: generate configurations for each hbase node;
    if [ $from -le 1 -a $to -ge 1 ] ; then
        gen_cfg_for_hbase_nodes $hbase_conf_dir
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_hbase(): failed to generate configuration files for each node"
            return 1
        fi
    fi

    #Step-2.a: if zookeeper is not included in this deployment, then zookeeper service must be available.
    if [ $from -le 2 -a $to -ge 2 ] ; then
        if [ "X$zk_included" != "Xtrue" ] ; then
            check_zk_service_for_hbase $hbase_conf_dir
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit deploy_hbase(): zookeeper service is not availabe"
                return 1
            fi
        fi
    fi

    for node in `cat $hbase_nodes` ; do
        local node_cfg=$hbase_comm_cfg
        [ -f $hbase_conf_dir/$node ] && node_cfg=$hbase_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local java_home=`grep "env:JAVA_HOME=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`

        log "INFO: in deploy_hbase(): node=$node node_cfg=$node_cfg"
        log "INFO:        user=$user"
        log "INFO:        ssh_port=$ssh_port"
        log "INFO:        java_home=$java_home"
        log "INFO:        install_path=$install_path"
        log "INFO:        package=$package"


        if [ "X$user" != "Xroot" ] ; then
            log "ERROR: Exit deploy_hbase(): currently, only 'root' user is supported user=$user"
            return 1
        fi

        #we have checked the package in gen_cfg_for_hbase_node() function: is name valid, if package exists ...
        local installation=`basename $package`
        installation=`echo $installation | sed -e 's/.tar.gz$//' -e 's/.tgz$//' -e 's/-bin//'`
        installation=$install_path/$installation

        #Step-2.b: check hbase nodes (such as java environment)
        if [ $from -le 2 -a $to -ge 2 ] ; then
            log "INFO: in deploy_hbase(): check hbase node $node ..."
            check_hbase_node "$node" "$user" "$ssh_port" "$java_home"
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit deploy_hbase(): check_hbase_node failed on $node"
                return 1
            fi
        fi

        #Step-3: stop hbase processes
        if [ $from -le 3 -a $to -ge 3 ] ; then
            log "INFO: in deploy_hbase(): stop hbase processes on $node ..."
            stop_hbase "$node" "$user" "$ssh_port" "$installation"
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit deploy_hbase(): stop_hbase failed on $node"
                return 1
            fi
        fi

        #Step-4: uninstall existing installation
        if [ $from -le 4 -a $to -ge 4 ] ; then
            log "INFO: in deploy_hbase(): uninstall hbase on $node ..."
            uninstall_hbase "$node" "$user" "$ssh_port" "$installation"
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit deploy_hbase(): uninstall_hbase failed on $node"
                return 1
            fi
        fi

        #Step-5: prepare environment
        if [ $from -le 5 -a $to -ge 5 ] ; then
            log "INFO: in deploy_hbase(): prepare hbase node $node ..."
            prepare_hbase_node "$hbase_conf_dir" "$node"
            if [ $? -ne 0 ] ; then
                log "ERROR: Exit deploy_hbase(): prepare_hbase_node failed on $node"
                return 1
            fi
        fi
    done

    #Step-6: dispatch hbase package to each node. Note that what's dispatched is the release-package, which doesn't
    #        contain our configurations. We will dispatch the configuation files later.
    if [ $from -le 6 -a $to -ge 6 ] ; then
        dispatch_hbase_package $hbase_conf_dir
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_hbase(): failed to dispatch hbase package to some node"
            return 1
        fi
    fi

    #Step-7: dispatch configurations to each hbase node;
    if [ $from -le 7 -a $to -ge 7 ] ; then
        dispatch_hbase_configs $hbase_conf_dir
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_hbase(): failed to dispatch configuration files to each node"
            return 1
        fi
    fi

    #Step-8: start hbase daemons on each hbase node;
    if [ $from -le 8 -a $to -ge 8 ] ; then
        start_hbase_daemons $hbase_conf_dir
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_hbase(): failed to start hbase daemons on some node"
            return 1
        fi

        log "INFO: in deploy_hbase(): sleep 10 seconds before checking hbase status ..."
        sleep 10 

        check_hbase_status $hbase_conf_dir
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_hbase(): failed to check hbase status"
            return 1
        fi
    fi

    log "INFO: Exit deploy_hbase(): Success"
    return 0
}

fi
