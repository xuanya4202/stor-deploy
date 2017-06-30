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

    [ ! -f $map ] && touch $map

    cat $map | grep "^$key=" > /dev/null 2>&1
    if [ $? -eq 0 ] ; then
        sed -i -e "s#^$key=.*#$key=$val#" $map || return 1
    else
        echo "$key=$val" >> $map || return 1
    fi

    return 0
}

function map_append()
{
    local map=$1
    local key=$2
    local val=$3

    [ ! -f $map ] && touch $map

    cat $map | grep "^$key=" > /dev/null 2>&1
    if [ $? -eq 0 ] ; then
        sed -i -e "/^$key=/ s|\$|,$val|" $map || return 1
    else
        echo "$key=$val" >> $map || return 1
    fi
    return 0
}

function include_module()
{
    local target=$1
    local modules=$2

    [ -z "$modules" ] && return 0    # if modules is empty, return true;

    echo $modules | grep $target > /dev/null 2>&1
    if [ $? -eq 0 ] ; then # $target is found in $modules, return true;
        return 0
    fi

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

    echo $line | grep "=" > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        log "WARN: put_kv_in_map(): no '=' found. line=\"$line\""
        return 2
    fi

    local key=`echo $line | cut -d '=' -f 1`
    local val=`echo $line | cut -d '=' -f 2-`

    key=`echo $key | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
    val=`echo $val | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`

    key=`echo $key | sed -e 's/[[:space:]]*:[[:space:]]*/:/'`      # " : " => ":"

    if [ -z "$key" -o -z "$val" ] ; then
        log "WARN: put_kv_in_map(): key or val is empty. line=\"$line\" key=\"$key\" val=\"$val\""
        return 2
    fi

    map_put "$map" "$key" "$val"   # the "" is necessary, because key/val may contain spaces
    if [ $? -ne 0 ] ; then
        log "ERROR: put_kv_in_map(): failed to put key-val in map. key=\"$key\" val=\"$val\" map=\"$map\""
        return 1
    fi

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

    echo $line | grep "=" > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        log "WARN: append_kv_in_map(): no '=' found. line=\"$line\""
        return 2
    fi

    local key=`echo $line | cut -d '=' -f 1`
    local val=`echo $line | cut -d '=' -f 2-`

    key=`echo $key | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
    val=`echo $val | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`

    key=`echo $key | sed -e 's/[[:space:]]*:[[:space:]]*/:/'`      # " : " => ":"

    if [ -z "$key" -o -z "$val" ] ; then
        log "WARN: append_kv_in_map(): key or val is empty. line=\"$line\" key=\"$key\" val=\"$val\""
        return 2
    fi

    map_append "$map" "$key" "$val"   # the "" is necessary, because key/val may contain spaces
    if [ $? -ne 0 ] ; then
        log "ERROR: append_kv_in_map(): failed to put key-val in map. key=\"$key\" val=\"$val\" map=\"$map\""
        return 1
    fi

    return 0
}


function parse_def_conf()
{
    local def_conf_file=$1      #default conf file
    local dest=$2               #the dest dir
    local modules=$3

    local comm_conf=""
    local curr_dir=""

    cat $def_conf_file | while read line ; do
        line=`echo $line | sed 's/#.*$//'`
        line=`echo $line | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
        [ -z "$line" ] && continue   #skip empty lines

        #start a block;
        if [ "$line" = "[COMMON]" ] ; then
            curr_dir=$dest
            mkdir -p $curr_dir || return 1
            comm_conf=$curr_dir/common
            rm -fr $comm_conf
        elif [ "$line" = "[ZK_COMMON]" ] ; then
            if include_module "zk" "$modules" ; then
                curr_dir=$dest/zk
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                rm -fr $comm_conf
                [ -f $dest/common ] && cp -f $dest/common $curr_dir
            else
                $curr_dir=""
            fi
        elif [ "$line" = "[HDFS_COMMON]" ] ; then
            if include_module "hdfs" "$modules" ; then
                curr_dir=$dest/hdfs
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                rm -fr $comm_conf
                [ -f $dest/common ] && cp -f $dest/common $curr_dir
            else
                $curr_dir=""
            fi
        elif [ "$line" = "[HBASE_COMMON]" ] ; then
            if include_module "hbase" "$modules" ; then
                curr_dir=$dest/hbase
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                rm -fr $comm_conf
                [ -f $dest/common ] && cp -f $dest/common $curr_dir
            else
                $curr_dir=""
            fi
        else
            [ -z "$curr_dir" ] && continue

            put_kv_in_map "$comm_conf" "$line"  # thie "" is necessary, because there may be spaces in $line
            local retCode=$?
            if [ $retCode -eq 1 ] ; then
                log "ERROR: parse_def_conf(): put_kv_in_map failed. def_conf_file=$def_conf_file"
                return 1
            elif [ $retCode -eq 2 ] ; then
                log "WARN: parse_def_conf(): put_kv_in_map succeeded with warnning. def_conf_file=$def_conf_file"
            fi
        fi
    done

    return 0
}

function parse_stor_conf()
{
    local stor_conf_file=$1      #conf file
    local dest=$2                #the dest dir
    local modules=$3

    local comm_conf=""
    local node_list=""
    local curr_dir=""

    local all_nodes=$dest/all-nodes
    local hdfs_all_nodes=$dest/hdfs/all-nodes
    local hbase_all_nodes=$dest/hbase/all-nodes
    rm -f $all_nodes $hdfs_all_nodes $hbase_all_nodes

    cat $stor_conf_file | while read line ; do
        line=`echo $line | sed 's/#.*$//'`
        line=`echo $line | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
        [ -z "$line" ] && continue   #skip empty lines

        #start a block;
        if [ "$line" = "[COMMON]" ] ; then
            curr_dir=$dest
            mkdir -p $curr_dir || return 1
            comm_conf=$curr_dir/common
            node_list=""
        elif [ "$line" = "[ZK_COMMON]" ] ; then
            if include_module "zk" "$modules" ; then
                curr_dir=$dest/zk
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=""
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
            else
                $curr_dir=""
            fi
        elif [ "$line" = "[ZK_NODES]" ] ; then
            if include_module "zk" "$modules" ; then
                curr_dir=$dest/zk
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list
            else
                $curr_dir=""
            fi
        elif [ "$line" = "[HDFS_COMMON]" ] ; then
            if include_module "hdfs" "$modules" ; then
                curr_dir=$dest/hdfs
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=""
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
            else
                $curr_dir=""
            fi
        elif [ "$line" = "[HDFS_NAME_NODES]" ] ; then
            if include_module "hdfs" "$modules" ; then
                curr_dir=$dest/hdfs
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/name-nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list
            else
                $curr_dir=""
            fi
        elif [ "$line" = "[HDFS_DATA_NODES]" ] ; then
            if include_module "hdfs" "$modules" ; then
                curr_dir=$dest/hdfs
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/data-nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list
            else
                $curr_dir=""
            fi
        elif [ "$line" = "[HDFS_JOURNAL_NODES]" ] ; then
            if include_module "hdfs" "$modules" ; then
                curr_dir=$dest/hdfs
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/journal-nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list
            else
                $curr_dir=""
            fi
        elif [ "$line" = "[HDFS_ZKFC_NODES]" ] ; then
            if include_module "hdfs" "$modules" ; then
                curr_dir=$dest/hdfs
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/zkfc-nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list
            else
                $curr_dir=""
            fi
        elif [ "$line" = "[HBASE_COMMON]" ] ; then
            if include_module "hbase" "$modules" ; then
                curr_dir=$dest/hbase
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=""
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
            else
                $curr_dir=""
            fi
        elif [ "$line" = "[HBASE_MASTER_NODES]" ] ; then
            if include_module "hbase" "$modules" ; then
                curr_dir=$dest/hbase
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/master-nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list
            else
                $curr_dir=""
            fi
        elif [ "$line" = "[HBASE_REGION_NODES]" ] ; then
            if include_module "hbase" "$modules" ; then
                curr_dir=$dest/hbase
                mkdir -p $curr_dir || return 1
                comm_conf=$curr_dir/common
                node_list=$curr_dir/region-nodes
                if [ ! -f $comm_conf ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $curr_dir
                fi
                rm -f $node_list
            else
                $curr_dir=""
            fi
        else
            [ -z "$curr_dir" ] && continue

            if [ -z "$node_list" ] ; then # we are in [*COMMON] block; only $comm_conf is defined;
                put_kv_in_map "$comm_conf" "$line"  # thie "" is necessary, because there may be spaces in $line
                local retCode=$?
                if [ $retCode -eq 1 ] ; then
                    log "ERROR: parse_def_conf(): put_kv_in_map failed. stor_conf_file=$stor_conf_file"
                    return 1
                elif [ $retCode -eq 2 ] ; then
                    log "WARN: parse_def_conf(): put_kv_in_map succeeded with warnning. stor_conf_file=$stor_conf_file"
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

                echo $node >> $node_list
                echo $node >> $all_nodes
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
                        else #there is no leading "extra:", replace the key with the new value;
                            put_kv_in_map "$node_conf_file" "$kv_pair"    # thie "" is necessary, because there may be spaces in $kv_pair
                        fi
                    done
                fi
            fi
        fi
    done

    # *nodes file may contain duplicated lines, dedup them!
    local tmpFile=`mktemp --suffix=-stor-deploy.tmp`
    for nodes_file in `find logs/ -name "*nodes" -type f` ; do
        cat $nodes_file | sort | uniq > $tmpFile
        mv -f $tmpFile $nodes_file
    done

    return 0
}

parse_def_conf stor-default.conf logs/test2 ""
parse_stor_conf stor.conf logs/test2 ""

fi
