#!/bin/bash

if [ -z "$parse_conf_defined" ] ; then
parse_conf_defined="true"

PARSE="${BASH_SOURCE-$0}"
PARSE_PDIR="$(dirname "${PARSE}")"
PARSE_PDIR="$(cd "${PARSE_PDIR}"; pwd)"

. $PARSE_PDIR/tools.sh

function map_put()
{
    local map=$1
    local key=$2
    local val=$3
    
    log "INFO: Enter function map_put(): map=$map key=$key val=$val"

    [ ! -f $map ] && touch $map

    cat $map | grep "^$key=" > /dev/null 2>&1
    if [ $? -eq 0 ] ; then
        sed -i -e "s#^$key=.*#$key=$val#" $map || return 1
    else
        echo "$key=$val" >> $map || return 1
    fi

    log "INFO: Exit function map_put(): Success"
    return 0
}

function map_append()
{
    local map=$1
    local key=$2
    local val=$3

    log "INFO: Enter function map_append(): map=$map key=$key val=$val"

    [ ! -f $map ] && touch $map

    cat $map | grep "^$key=" > /dev/null 2>&1
    if [ $? -eq 0 ] ; then
        sed -i -e "/^$key=/ s|\$|,$val|" $map || return 1
    else
        echo "$key=$val" >> $map || return 1
    fi

    log "INFO: Exit function map_append(): Success"
    return 0
}

function include_module()
{
    local target=$1
    local modules=$2

    log "INFO: Enter function include_module(): target=$target modules=$modules"

    echo $modules | grep $target > /dev/null 2>&1
    if [ $? -eq 0 ] ; then # $target is found in $modules, return true;
        log "INFO: Exit function include_module(): $target included"
        return 0
    fi

    log "INFO: Exit function include_module(): $target not Included"
    return 1
}

#return:
#   0 : success
#   1 : failure
#   2 : warn
function put_kv_in_map()
{
    local map=$1
    local line=$2

    log "INFO: Enter function put_kv_in_map(): map=$map line=\"$line\""

    echo $line | grep "=" > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        log "WARN: Exit function put_kv_in_map(): no '=' found in line. line=\"$line\""
        return 2
    fi

    local key=`echo $line | cut -d '=' -f 1`
    local val=`echo $line | cut -d '=' -f 2-`

    key=`echo $key | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
    val=`echo $val | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`

    key=`echo $key | sed -e 's/[[:space:]]*:[[:space:]]*/:/'`      # " : " => ":"

    if [ -z "$key" -o -z "$val" ] ; then
        log "WARN: Exit function put_kv_in_map(): key or val is empty. line=\"$line\" key=\"$key\" val=\"$val\""
        return 2
    fi

    map_put "$map" "$key" "$val"   # the "" is necessary, because key/val may contain spaces
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit function put_kv_in_map(): failed to put key-val in map. key=\"$key\" val=\"$val\" map=\"$map\""
        return 1
    fi

    log "INFO: Exit function put_kv_in_map(): Success"
    return 0
}


#return:
#   0 : success
#   1 : failure
#   2 : warn
function append_kv_in_map()
{
    local map=$1
    local line=$2

    log "INFO: Enter function append_kv_in_map(): map=$map line=\"$line\""

    echo $line | grep "=" > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        log "WARN: Exit function append_kv_in_map(): no '=' found. line=\"$line\""
        return 2
    fi

    local key=`echo $line | cut -d '=' -f 1`
    local val=`echo $line | cut -d '=' -f 2-`

    key=`echo $key | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
    val=`echo $val | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`

    key=`echo $key | sed -e 's/[[:space:]]*:[[:space:]]*/:/'`      # " : " => ":"

    if [ -z "$key" -o -z "$val" ] ; then
        log "WARN: Exit function append_kv_in_map(): key or val is empty. line=\"$line\" key=\"$key\" val=\"$val\""
        return 2
    fi

    map_append "$map" "$key" "$val"   # the "" is necessary, because key/val may contain spaces
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit function append_kv_in_map(): failed to put key-val in map. key=\"$key\" val=\"$val\" map=\"$map\""
        return 1
    fi

    log "INFO: Exit function append_kv_in_map(): Success"
    return 0
}


function parse_def_conf()
{
    local def_conf_file=$1      #default conf file
    local dest=$2               #the dest dir
    local modules=$3

    log "INFO: Enter function parse_def_conf(): def_conf_file=$def_conf_file dest=$dest modules=$modules"

    local comm_conf=""
    local curr_dir=""

    cat $def_conf_file | while read line ; do
        line=`echo $line | sed 's/#.*$//'`
        line=`echo $line | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
        [ -z "$line" ] && continue   #skip empty lines

        #start a block;
        if [ "$line" = "[COMMON]" ] ; then
            log "INFO: in function parse_def_conf(): enter [COMMON] block"
            curr_dir=$dest
            mkdir -p $curr_dir || return 1
            comm_conf=$curr_dir/common
            rm -fr $comm_conf || return 1
        elif [ "$line" = "[ZK_COMMON]" ] ; then
            if include_module "zk" "$modules" ; then
                log "INFO: in function parse_def_conf(): enter [ZK_COMMON] block"
                curr_dir=$dest/zk
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                rm -fr $comm_conf || return 1
                [ -f $dest/common ] && cp -f $dest/common $curr_dir
            else
                log "INFO: in function parse_def_conf(): skip [ZK_COMMON] block"
                curr_dir=""
            fi
        elif [ "$line" = "[HDFS_COMMON]" ] ; then
            if include_module "hdfs" "$modules" ; then
                log "INFO: in function parse_def_conf(): enter [HDFS_COMMON] block"
                curr_dir=$dest/hdfs
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                rm -fr $comm_conf || return 1
                [ -f $dest/common ] && cp -f $dest/common $curr_dir
            else
                log "INFO: in function parse_def_conf(): skip [HDFS_COMMON] block"
                curr_dir=""
            fi
        elif [ "$line" = "[HBASE_COMMON]" ] ; then
            if include_module "hbase" "$modules" ; then
                log "INFO: in function parse_def_conf(): enter [HBASE_COMMON] block"
                curr_dir=$dest/hbase
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                rm -fr $comm_conf || return 1
                [ -f $dest/common ] && cp -f $dest/common $curr_dir
            else
                log "INFO: in function parse_def_conf(): skip [HBASE_COMMON] block"
                curr_dir=""
            fi
        else
            [ -z "$curr_dir" ] && continue

            put_kv_in_map "$comm_conf" "$line"  # thie "" is necessary, because there may be spaces in $line

            local retCode=$?
            if [ $retCode -eq 1 ] ; then
                log "ERROR: Exit function parse_def_conf(): put_kv_in_map failed. def_conf_file=$def_conf_file"
                return 1
            elif [ $retCode -eq 2 ] ; then
                log "WARN: in function parse_def_conf(): put_kv_in_map succeeded with warnning. def_conf_file=$def_conf_file"
            fi
        fi
    done

    log "INFO: Exit function parse_def_conf(): success"
    return 0
}

function parse_stor_conf()
{
    local stor_conf_file=$1      #conf file
    local dest=$2                #the dest dir
    local modules=$3

    log "INFO: Enter function parse_stor_conf(): stor_conf_file=$stor_conf_file dest=$dest modules=$modules"

    local comm_conf=""
    local node_list=""
    local curr_dir=""

    local all_nodes=$dest/all-nodes
    local hdfs_all_nodes=$dest/hdfs/all-nodes
    local hbase_all_nodes=$dest/hbase/all-nodes
    rm -f $all_nodes $hdfs_all_nodes $hbase_all_nodes || return 1

    cat $stor_conf_file | while read line ; do
        line=`echo $line | sed 's/#.*$//'`
        line=`echo $line | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
        [ -z "$line" ] && continue   #skip empty lines

        #start a block;
        if [ "$line" = "[COMMON]" ] ; then
            log "INFO: in function parse_stor_conf(): enter [COMMON] block"
            curr_dir=$dest
            mkdir -p $curr_dir || return 1
            comm_conf=$curr_dir/common
            node_list=""
        elif [ "$line" = "[ZK_COMMON]" ] ; then
            if include_module "zk" "$modules" ; then
                log "INFO: in function parse_stor_conf(): enter [ZK_COMMON] block"
                curr_dir=$dest/zk
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=""
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
            else
                log "INFO: in function parse_stor_conf(): skip [ZK_COMMON] block"
                curr_dir=""
            fi
        elif [ "$line" = "[ZK_NODES]" ] ; then
            if include_module "zk" "$modules" ; then
                log "INFO: in function parse_stor_conf(): enter [ZK_NODES] block"
                curr_dir=$dest/zk
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list || return 1
            else
                log "INFO: in function parse_stor_conf(): skip [ZK_NODES] block"
                curr_dir=""
            fi
        elif [ "$line" = "[HDFS_COMMON]" ] ; then
            if include_module "hdfs" "$modules" ; then
                log "INFO: in function parse_stor_conf(): enter [HDFS_COMMON] block"
                curr_dir=$dest/hdfs
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=""
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
            else
                log "INFO: in function parse_stor_conf(): skip [HDFS_COMMON] block"
                curr_dir=""
            fi
        elif [ "$line" = "[HDFS_NAME_NODES]" ] ; then
            if include_module "hdfs" "$modules" ; then
                log "INFO: in function parse_stor_conf(): enter [HDFS_NAME_NODES] block"
                curr_dir=$dest/hdfs
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/name-nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list || return 1
            else
                log "INFO: in function parse_stor_conf(): skip [HDFS_NAME_NODES] block"
                curr_dir=""
            fi
        elif [ "$line" = "[HDFS_DATA_NODES]" ] ; then
            if include_module "hdfs" "$modules" ; then
                log "INFO: in function parse_stor_conf(): enter [HDFS_DATA_NODES] block"
                curr_dir=$dest/hdfs
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/data-nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list || return 1
            else
                log "INFO: in function parse_stor_conf(): skip [HDFS_DATA_NODES] block"
                curr_dir=""
            fi
        elif [ "$line" = "[HDFS_JOURNAL_NODES]" ] ; then
            if include_module "hdfs" "$modules" ; then
                log "INFO: in function parse_stor_conf(): enter [HDFS_JOURNAL_NODES] block"
                curr_dir=$dest/hdfs
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/journal-nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list || return 1
            else
                log "INFO: in function parse_stor_conf(): skip [HDFS_JOURNAL_NODES] block"
                curr_dir=""
            fi
        elif [ "$line" = "[HDFS_ZKFC_NODES]" ] ; then
            if include_module "hdfs" "$modules" ; then
                log "INFO: in function parse_stor_conf(): enter [HDFS_ZKFC_NODES] block"
                curr_dir=$dest/hdfs
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/zkfc-nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list || return 1
            else
                log "INFO: in function parse_stor_conf(): skip [HDFS_ZKFC_NODES] block"
                curr_dir=""
            fi
        elif [ "$line" = "[HBASE_COMMON]" ] ; then
            if include_module "hbase" "$modules" ; then
                log "INFO: in function parse_stor_conf(): enter [HBASE_COMMON] block"
                curr_dir=$dest/hbase
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=""
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
            else
                log "INFO: in function parse_stor_conf(): skip [HBASE_COMMON] block"
                curr_dir=""
            fi
        elif [ "$line" = "[HBASE_MASTER_NODES]" ] ; then
            if include_module "hbase" "$modules" ; then
                log "INFO: in function parse_stor_conf(): enter [HBASE_MASTER_NODES] block"
                curr_dir=$dest/hbase
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/master-nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list || return 1
            else
                log "INFO: in function parse_stor_conf(): skip [HBASE_MASTER_NODES] block"
                curr_dir=""
            fi
        elif [ "$line" = "[HBASE_REGION_NODES]" ] ; then
            if include_module "hbase" "$modules" ; then
                log "INFO: in function parse_stor_conf(): enter [HBASE_REGION_NODES] block"
                curr_dir=$dest/hbase
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/region-nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list || return 1
            else
                log "INFO: in function parse_stor_conf(): skip [HBASE_REGION_NODES] block"
                curr_dir=""
            fi
        else
            [ -z "$curr_dir" ] && continue

            if [ -z "$node_list" ] ; then # we are in [*COMMON] block; only $comm_conf is defined;

                if [ "$curr_dir" = "$dest" ] ; then
                    # we are in [COMMON] block of stor.conf. Notice that this block should overwrite all
                    # [*COMMON] blocks in stor-default.conf.
                    for mapfile in `find $dest -name "common" -type f` ; do
                        put_kv_in_map "$mapfile" "$line"  # thie "" is necessary, because there may be spaces in $line
                        local retCode=$?
                        if [ $retCode -eq 1 ] ; then
                            log "ERROR: Exit function parse_stor_conf(): put_kv_in_map failed. mapfile=$mapfile line=$line"
                            return 1
                        elif [ $retCode -eq 2 ] ; then
                            log "WARN: in function parse_stor_conf(): put_kv_in_map succeeded with warnning. mapfile=$mapfile line=$line"
                        fi
                    done
                else
                    # we are in [*_COMMON] block of stor.conf
                    put_kv_in_map "$comm_conf" "$line"  # thie "" is necessary, because there may be spaces in $line
                    local retCode=$?
                    if [ $retCode -eq 1 ] ; then
                        log "ERROR: Exit function parse_stor_conf(): put_kv_in_map failed. comm_conf=$comm_conf line=$line"
                        return 1
                    elif [ $retCode -eq 2 ] ; then
                        log "WARN: in function parse_stor_conf(): put_kv_in_map succeeded with warnning comm_conf=$comm_conf line=$line"
                    fi
                fi

            else  # we are in [*NODES] block; both $comm_conf and $node_list are defined;
                local node=""
                local node_conf=""

                echo $line | grep "?" > /dev/null 2>&1
                if [ $? -eq 0 ] ; then  # has node specific conf
                    node=`echo $line | cut -d '?' -f 1`
                    node_conf=`echo $line | cut -d '?' -f 2-`

                    node=`echo $node | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
                    node_conf=`echo $node_conf | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
                else
                    node=$line
                fi

                echo $node >> $node_list || return 1
                echo $node >> $all_nodes || return 1

                [ "$dest/hdfs" = "$curr_dir" ] && echo $node >> $hdfs_all_nodes
                [ "$dest/hbase" = "$curr_dir" ] && echo $node >> $hbase_all_nodes

                if [ -n "$node_conf" ] ; then
                    local node_conf_file=$curr_dir/$node   #the node-conf-file is named $node, such as 192.168.100.131
                    [ ! -f $node_conf_file ] && cp -f $comm_conf $node_conf_file

                    while [ -n "$node_conf" ] ; do
                        local kv_pair=""

                        echo $node_conf | grep "&"  > /dev/null 2>&1
                        if [ $? -eq 0 ] ; then  # '&' found
                            kv_pair=`echo $node_conf | cut -d '&' -f 1`
                            node_conf=`echo $node_conf | cut -d '&' -f 2-`

                            kv_pair=`echo $kv_pair | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
                            node_conf=`echo $node_conf | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
                        else
                            kv_pair=$node_conf
                            node_conf=""
                        fi

                        echo "$kv_pair" | grep "extra[[:space:]]*:[[:space:]]*" > /dev/null 2>&1
                        if [ $? -eq 0 ] ; then  # there is leading "extra:", so append the value to the key;
                            kv_pair=`echo "$kv_pair" | sed -e 's/extra[[:space:]]*:[[:space:]]*//'`   #chop the leading "extra:"
                            append_kv_in_map "$node_conf_file" "$kv_pair"    # thie "" is necessary, because there may be spaces in $kv_pair
                            local retCode=$?
                            if [ $retCode -eq 1 ] ; then
                                log "ERROR: Exit function parse_stor_conf(): append_kv_in_map failed. node_conf_file=$node_conf_file kv_pair=$kv_pair"
                                return 1
                            elif [ $retCode -eq 2 ] ; then
                                log "WARN: in function parse_stor_conf(): append_kv_in_map succeeded with warnning. node_conf_file=$node_conf_file kv_pair=$kv_pair"
                            fi
                        else #there is no leading "extra:", replace the key with the new value;
                            put_kv_in_map "$node_conf_file" "$kv_pair"    # thie "" is necessary, because there may be spaces in $kv_pair
                            local retCode=$?
                            if [ $retCode -eq 1 ] ; then
                                log "ERROR: Exit function parse_stor_conf(): put_kv_in_map failed. node_conf_file=$node_conf_file kv_pair=$kv_pair"
                                return 1
                            elif [ $retCode -eq 2 ] ; then
                                log "WARN: in function parse_stor_conf(): put_kv_in_map succeeded with warnning. node_conf_file=$node_conf_file kv_pair=$kv_pair"
                            fi
                        fi
                    done
                fi
            fi
        fi
    done

    # *nodes file may contain duplicated lines, dedup them!
    local tmpFile=`mktemp --suffix=-stor-deploy.tmp`
    for nodes_file in `find $dest -name "*nodes" -type f` ; do
        log "INFO: in function parse_stor_conf(): deduplicate $nodes_file"
        cat $nodes_file | sort | uniq > $tmpFile || return 1
        mv -f $tmpFile $nodes_file || return 1
    done

    log "INFO: Exit function parse_stor_conf(): Success"
    return 0
}

function parse_configuration()
{
    local def_conf=$1
    local conf=$2
    local dest_dir=$3
    local modules=$4

    log "INFO: Enter function parse_configuration(): def_conf=$def_conf conf=$conf dest_dir=$dest_dir modules=$modules"

    if [ -z "$def_conf" -o -z "$conf" -o -z "$dest_dir" -o -z "$modules" ] ; then
        log "ERROR: Exit function parse_configuration(): one or more parameters is missing"
        return 1
    fi

    if [ ! -f $def_conf ] ; then
        log "ERROR: Exit function parse_configuration(): default config file $def_conf doesn't exist"
        return 1
    fi

    if [ ! -f $conf ] ; then
        log "ERROR: Exit function parse_configuration(): config file $conf doesn't exist"
        return 1
    fi

    if [ -f $dest_dir ] ; then
        log "ERROR: Exit function parse_configuration(): dest dir $dest_dir exists and it's a file"
        return 1
    fi

    rm -fr $dest_dir  || return 1
    mkdir -p $dest_dir || return 1

    parse_def_conf "$def_conf" "$dest_dir" "$modules"
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit function parse_configuration(): parse_def_conf failed"
        return 1
    fi

    parse_stor_conf "$conf" "$dest_dir" "$modules"
    if [ $? -ne 0 ] ; then
        log "ERROR: Exit function parse_configuration(): parse_stor_conf failed"
        return 1
    fi

    log "INFO: Exit function parse_configuration(): Success"
    return 0
}

fi
