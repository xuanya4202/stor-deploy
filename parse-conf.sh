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

    map_put $map "$key" "$val"   # the "" is necessary, because key/val may contain spaces
    if [ $? -ne 0 ] ; then
        log "ERROR: put_kv_in_map(): failed to put key-val in map. key=\"$key\" val=\"$val\" map=\"$map\""
        return 1
    fi

    return 0
}

function parse_def_conf()
{
    local conf=$1      #default conf file
    local dest=$2      #the dest dir
    local modules=$3

    local map=""

    cat $conf | while read line ; do
        line=`echo $line | sed 's/#.*$//'`
        line=`echo $line | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
        [ -z "$line" ] && continue   #skip empty lines

        #start a block;
        if [ "$line" = "[COMMON]" ] ; then
            mkdir -p $dest || return 1
            map=$dest/common
            rm -fr $map
        elif [ "$line" = "[ZK_COMMON]" ] ; then
            if include_module "zk" $modules ; then
                mkdir -p $dest/zk || return 1
                map=$dest/zk/common
                rm -fr $map
                [ -f $dest/common ] && cp -f $dest/common $map
            else
                map=""
            fi
        elif [ "$line" = "[HDFS_COMMON]" ] ; then
            if include_module "hdfs" $modules ; then
                mkdir -p $dest/hdfs || return 1
                map=$dest/hdfs/common
                rm -fr $map
                [ -f $dest/common ] && cp -f $dest/common $map
            else
                map=""
            fi
        elif [ "$line" = "[HBASE_COMMON]" ] ; then
            if include_module "hbase" $modules ; then
                mkdir -p $dest/hbase || return 1
                map=$dest/hbase/common
                rm -fr $map
                [ -f $dest/common ] && cp -f $dest/common $map
            else
                map=""
            fi
        else
            [ -z "$map" ] && continue

            put_kv_in_map $map "$line"  # thie "" is necessary, because there may be spaces in $line
            local retCode=$?
            if [ $retCode -eq 1 ] ; then
                log "ERROR: parse_def_conf(): put_kv_in_map failed. conf=$conf"
                return 1
            elif [ $retCode -eq 2 ] ; then
                log "WARN: parse_def_conf(): put_kv_in_map succeeded with warnning. conf=$conf"
            fi
        fi
    done

    return 0
}

function parse_stor_conf()
{
    local conf=$1      #conf file
    local dest=$2      #the dest dir

    local map=""
    local list=""

    local all_nodes=$dest/all_nodes
    rm -f $all_nodes

    cat $conf | while read line ; do
        line=`echo $line | sed 's/#.*$//'`
        line=`echo $line | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
        [ -z "$line" ] && continue   #skip empty lines

        #start a block;
        if [ "$line" = "[COMMON]" ] ; then
            mkdir -p $dest || return 1
            map=$dest/common
            list=""
        elif [ "$line" = "[ZK_COMMON]" ] ; then
            if include_module "zk" $modules ; then
                mkdir -p $dest/zk || return 1
                map=$dest/zk/common
                list=""
                if [ ! -f $map ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $map
                fi
            else
                map=""
                list=""
            fi
        elif [ "$line" = "[ZK_NODES]" ] ; then
            if include_module "zk" $modules ; then
                mkdir -p $dest/zk || return 1
                map=""
                list=$dest/zk/nodes
                rm -f $list
            else
                map=""
                list=""
            fi
        elif [ "$line" = "[HDFS_COMMON]" ] ; then
            if include_module "hdfs" $modules ; then
                mkdir -p $dest/hdfs || return 1
                map=$dest/hdfs/common
                list=""
                if [ ! -f $map ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $map 
                fi
            else
                map=""
                list=""
            fi
        elif [ "$line" = "[HDFS_NAME_NODES]" ] ; then
            if include_module "hdfs" $modules ; then
                mkdir -p $dest/hdfs || return 1
                map=""
                list=$dest/hdfs/name-nodes
                rm -f $list
            else
                map=""
                list=""
            fi
        elif [ "$line" = "[HDFS_DATA_NODES]" ] ; then
            if include_module "hdfs" $modules ; then
                mkdir -p $dest/hdfs || return 1
                map=""
                list=$dest/hdfs/data-nodes
                rm -f $list
            else
                map=""
                list=""
            fi
        elif [ "$line" = "[HDFS_JOURNAL_NODES]" ] ; then
            if include_module "hdfs" $modules ; then
                mkdir -p $dest/hdfs || return 1
                map=""
                list=$dest/hdfs/journal-nodes
                rm -f $list
            else
                map=""
                list=""
            fi
        elif [ "$line" = "[HDFS_ZKFC_NODES]" ] ; then
            if include_module "hdfs" $modules ; then
                mkdir -p $dest/hdfs || return 1
                map=""
                list=$dest/hdfs/zkfc-nodes
                rm -f $list
            else
                map=""
                list=""
            fi
        elif [ "$line" = "[HBASE_COMMON]" ] ; then
            if include_module "hbase" $modules ; then
                mkdir -p $dest/hbase || return 1
                map=$dest/hbase/common
                list=""
                if [ ! -f $map ] ; then
                    [ -f $dest/common ] && cp -f $dest/common $map
                fi
            else
                map=""
                list=""
            fi
        elif [ "$line" = "[HBASE_MASTER_NODES]" ] ; then
            if include_module "hbase" $modules ; then
                mkdir -p $dest/hbase || return 1
                map=""
                list=$dest/hbase/master-nodes
                rm -f $list
            else
                map=""
                list=""
            fi
        elif [ "$line" = "[HBASE_REGION_NODES]" ] ; then
            if include_module "hbase" $modules ; then
                mkdir -p $dest/hbase || return 1
                map=""
                list=$dest/hbase/region-nodes
                rm -f $list
            else
                map=""
                list=""
            fi
        else
            [ -z "$map" -a -z "$list" ] && continue

            if [ -n "$map" -a -n "$list" ] ; then
                log "ERROR: parse_stor_conf(): in two blocks at the same time?"
                return 1
            fi

            if [ -n "$map" ] ; then # we are in [*COMMON] block
                put_kv_in_map $map "$line"  # thie "" is necessary, because there may be spaces in $line
                local retCode=$?
                if [ $retCode -eq 1 ] ; then
                    log "ERROR: parse_def_conf(): put_kv_in_map failed. conf=$conf"
                    return 1
                elif [ $retCode -eq 2 ] ; then
                    log "WARN: parse_def_conf(): put_kv_in_map succeeded with warnning. conf=$conf"
                fi
            else  # we are in [*_NODES] block
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

                echo $node >> $list
                echo $node >> $all_nodes
            fi
        fi
    done
}

parse_def_conf stor-default.conf logs/test2 ""

fi
