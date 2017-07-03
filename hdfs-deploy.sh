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
        local key=`echo $line | cut -d '=' -f 1`
        local val=`echo $line | cut -d '=' -f 2-`

        if [ -z "$key" -o -z "$val" ] ; then
            log "INFO: in gen_cfg_for_hdfs_node(): key or val is empty: line=$line key=$key val=$val"
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
        local key=`echo $line | cut -d '=' -f 1`
        local val=`echo $line | cut -d '=' -f 2-`

        if [ -z "$key" -o -z "$val" ] ; then
            log "INFO: in gen_cfg_for_hdfs_node(): key or val is empty: line=$line key=$key val=$val"
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

    if [ ! -f $package ] ; then
        log "ERROR: Exit gen_cfg_for_hdfs_node(): hadoop package doesn't exist. package=$package"
        return 1
    fi

    local hadoopdir=`basename $package`
    hadoopdir=`echo $hadoopdir | sed -e 's/.tar.gz$//' -e 's/.tgz$//'`

    local extract_dir=/tmp/hdfs-extract-$run_timestamp

    local src_hadoop_env=$extract_dir/$hadoopdir/etc/hadoop/hadoop-env.sh
    local src_slave_sh=$extract_dir/$hadoopdir/sbin/slaves.sh
    local src_daemon_sh=$extract_dir/$hadoopdir/sbin/hadoop-daemon.sh

    if [ ! -f $src_hadoop_env -o ! -f $src_slave_sh -o ! -f $src_daemon_sh ] ; then 
        log "INFO: in gen_cfg_for_hdfs_node(): unzip hadoop package. package=$package ..."

        rm -fr $extract_dir || return 1
        mkdir -p $extract_dir || return 1

        tar -C $extract_dir -zxf $package  \
                                 $hadoopdir/etc/hadoop/hadoop-env.sh   \
                                 $hadoopdir/sbin/slaves.sh             \
                                 $hadoopdir/sbin/hadoop-daemon.sh || return 1

        if [ ! -f $src_hadoop_env -o ! -f $src_slave_sh -o ! -f $src_daemon_sh ] ; then 
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
        sed -i -e "2 i $line" $hadoop_env || return 1
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

function deploy_hdfs()
{
    local parsed_conf_dir=$1
    local operation=$2

    log "INFO: Enter deploy_hdfs(): parsed_conf_dir=$parsed_conf_dir"

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

    if [ "X$operation" = "Xparse" ] ; then
        log "INFO: Exit deploy_hdfs(): stop early because operation=$operation"
        return 0
    fi

    #Step-2: check hdfs nodes (such as java environment), clean up hdfs nodes, and prepare (mount the disks, create the dirs)
    for node in `cat $hdfs_nodes` ; do
        local node_cfg=$hdfs_comm_cfg
        [ -f $hdfs_conf_dir/$node ] ; node_cfg=$hdfs_conf_dir/$node

        local user=`grep "user=" $node_cfg | cut -d '=' -f 2-`
        local ssh_port=`grep "ssh_port=" $node_cfg | cut -d '=' -f 2-`
        local java_home=`grep "env:JAVA_HOME=" $node_cfg | cut -d '=' -f 2-`
        local install_path=`grep "install_path=" $node_cfg | cut -d '=' -f 2-`
        local package=`grep "package=" $node_cfg | cut -d '=' -f 2-`
        local mounts=`grep "mounts=" $node_cfg | cut -d '=' -f 2-`
        local mkfs_cmd=`grep "mkfs_cmd=" $node_cfg | cut -d '=' -f 2-`
        local mount_opts=`grep "mount_opts=" $node_cfg | cut -d '=' -f 2-`


        log "INFO: in deploy_hdfs(): node=$node node_cfg=$node_cfg"
        log "INFO:        user=$user"
        log "INFO:        ssh_port=$ssh_port"
        log "INFO:        java_home=$java_home"
        log "INFO:        install_path=$install_path"
        log "INFO:        package=$package"
        log "INFO:        mounts=$mounts"
        log "INFO:        mkfs_cmd=$mkfs_cmd"
        log "INFO:        mount_opts=$mount_opts"

        if [ "X$user" != "Xroot" ] ; then
            log "ERROR: Exit deploy_hdfs(): currently, only 'root' user is allowed. user=$user"
            return 1
        fi

        #we have checked the package in gen_cfg_for_hdfs_node() function: is name valid, if package exists ...
        local installation=`basename $package`
        installation=`echo $installation | sed -e 's/.tar.gz$//' -e 's/.tgz$//'`
        installation=$install_path/$installation

        #check the node, such as java environment
        log "INFO: in deploy_hdfs(): check hdfs node $node ..."
        check_hdfs_node "$node" "$user" "$ssh_port" "$java_home"

        [ "X$operation" = "Xcheck" ] && continue

        #clean up hdfs node
        log "INFO: in deploy_hdfs(): clean up hdfs node $node ..."
        clean_up_hdfs_node "$node" "$user" "$ssh_port" "$installation"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_hdfs(): clean_up_hdfs_node failed on $node"
            return 1
        fi

        [ "X$operation" = "Xclean" ] && continue

        #prepare environment
        log "INFO: in deploy_hdfs(): prepare hdfs node $node ..."
        prepare_hdfs_node "$node" "$user" "$ssh_port" "$dataDir" "$dataLogDir" "$logdir" "$pidfile" "$install_path" "$mounts" "$mount_opts" "$mkfs_cmd"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit deploy_hdfs(): prepare_hdfs_node failed on $node"
            return 1
        fi
    done

    if [ "X$operation" = "Xcheck" -o "X$operation" = "Xclean" -o "X$operation" = "Xprepare" ] ; then
        log "INFO: Exit deploy_hdfs(): stop early because operation=$operation"
        return 0
    fi

    #Step-3: dispatch hdfs package to each node. Note that what's dispatched is the release-package, which doesn't
    #        contain our configurations. We will dispatch the configuation files later.
    dispatch_hdfs_package $hdfs_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_hdfs(): failed to dispatch hadoop package to some node"
        return 1
    fi

    #Step-4: dispatch configurations to each hdfs node;
    dispatch_hdfs_configs $hdfs_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_hdfs(): failed to dispatch configuration files to each node"
        return 1
    fi

    if [ "X$operation" = "Xinstall" ] ; then
        log "INFO: Exit deploy_hdfs(): stop early because operation=$operation"
        return 0
    fi

    #Step-5: start hdfs daemons on each hdfs node;
    start_hdfs_daemons $hdfs_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_hdfs(): failed to start hdfs daemons on some node"
        return 1
    fi

    #Step-6: check hdfs status;
    check_hdfs_status $hdfs_conf_dir
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit deploy_hdfs(): failed to check hadfs status"
        return 1
    fi

    log "INFO: Exit deploy_hdfs(): Success"
    return 0
}

fi
