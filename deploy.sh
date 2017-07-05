#!/bin/bash

SCRIPT="${BASH_SOURCE-$0}"
SCRIPT_PDIR="$(dirname "${SCRIPT}")"
SCRIPT_PDIR="$(cd "${SCRIPT_PDIR}"; pwd)"

LOGS=$SCRIPT_PDIR/logs
mkdir -p $LOGS
LOG_FILE=$LOGS/deploy.log

. $SCRIPT_PDIR/tools.sh
. $SCRIPT_PDIR/parse-conf.sh
. $SCRIPT_PDIR/zk-deploy.sh
. $SCRIPT_PDIR/hdfs-deploy.sh
. $SCRIPT_PDIR/hbase-deploy.sh

run_timestamp=`date +%Y%m%d%H%M%S`

modules=
operation=
stop_after=
up_what=

zk_included=
hdfs_included=
hbase_included=

function usage()
{
    echo "Usage: $0 -m {modules} -o {operation} [-s {stop-after}] [-u {upgrade-what}]"
    echo "    -m modules      : one or more modules in zk, hdfs and hbase, separated by comma"
    echo "                      such as 'zk' or 'zk,hdfs' or 'zk,hdfs,hbase'"
    echo "    -o operation    : deploy|upgrade"
    echo "    -s stop-after   : parse|check|clean|prepare|install|all"
    echo "                      only useful when operation=deploy; by default, stop-after=parse"
    echo "    -u upgrade-what : conf|package"
    echo "                      only useful when operation=upgrade; by default, upgrade-what=conf"
}


while getopts "m:o:s:u:h" opt ; do
    case $opt in 
        m)
            modules=$OPTARG
            ;;
        o)
            operation=$OPTARG
            ;;
        s)
            stop_after=$OPTARG
            ;;
        u)
            up_what=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done

if [ -z "$modules" -o -z "$operation" ] ; then
    echo "ERROR: argument 'modules' and 'operation' must be present!"
    usage
    exit 1
fi

for m in `echo $modules | sed -e 's/,/ /g'` ; do
    if [ "X$m" = "Xzk" ] ; then
        zk_included="true"
        continue
    fi

    if [ "X$m" = "Xhdfs" ] ; then
        hdfs_included="true"
        continue
    fi

    if [ "X$m" = "Xhbase" ] ; then
        hbase_included="true"
        continue
    fi

    echo "ERROR: argument 'modules' is invalid: $m is unrecognized, it must be 'zk', 'hdfs' or 'hbase'"
    usage
    exit 1
done

if [ X"$operation" != "Xdeploy" -a X"$operation" != "Xupgrade" ] ; then
    echo "ERROR: argument 'operation' is invalid: it must be 'deploy' or 'upgrade'"
    usage
    exit 1
fi

if [ -n "$stop_after" -a  X"$operation" != "Xdeploy" ] ; then
    echo "ERROR: 'stop-after' can be present only when operation=deploy"
    usage
    exit 1
fi

if [ -n "$up_what" -a  X"$operation" != "Xupgrade" ] ; then
    echo "ERROR: 'upgrade-what' can be present only when operation=upgrade"
    usage
    exit 1
fi

[ X"$operation" = "Xdeploy" -a -z "$stop_after" ] && stop_after=parse
[ X"$operation" = "Xupgrade" -a -z "$up_what" ] && up_what=conf


log " "
log " "
log "INFO: ========================================== `basename $0` =========================================="



if [ X"$operation" = "Xdeploy" ] ; then
    log "INFO: run_timestamp=$run_timestamp modules=$modules operation=$operation stop_after=$stop_after"

    log "INFO: zk_included=$zk_included"
    log "INFO: hdfs_included=$hdfs_included"
    log "INFO: hbase_included=$hbase_included"

    if [ "X$stop_after" = "Xclean" -o "X$stop_after" = "Xprepare" -o "X$stop_after" = "Xinstall" -o "X$stop_after" = "Xall" ] ; then
        log "operation=$operation and stop_after=$stop_after, so data will be cleared, are you sure? [yes/no]"
        read answer
        if [ X"$answer" != "Xyes" ] ; then
            log "Your answer ($answer) is not 'yes', exit!"
            exit 0
        else
            log "Your answer is 'yes', continue!"
        fi
    fi

    parse_configuration $SCRIPT_PDIR/conf/stor-default.conf $SCRIPT_PDIR/conf/stor.conf $LOGS/deploy-$run_timestamp "$modules"
    if [ $? -ne 0 ] ; then
        log "ERROR: parse_configuration failed"
        exit 1
    fi

    if [ "X$zk_included" = "Xtrue" ] ; then
        deploy_zk "$LOGS/deploy-$run_timestamp" "$stop_after"
        if [ $? -ne 0 ] ; then
            log "ERROR: deploy_zk failed"
            exit 1
        fi
    fi
    
    if [ "X$hdfs_included" = "Xtrue" ] ; then
        deploy_hdfs "$LOGS/deploy-$run_timestamp" "$stop_after" "$zk_included"
        if [ $? -ne 0 ] ; then
            log "ERROR: deploy_hdfs failed"
            exit 1
        fi
    fi
    
    if [ "X$hbase_included" = "Xtrue" ] ; then
        deploy_hbase "$LOGS/deploy-$run_timestamp" "$stop_after" "$zk_included"
        if [ $? -ne 0 ] ; then
            log "ERROR: deploy_hbase failed"
            exit 1
        fi
    fi
else
    log "INFO: run_timestamp=$run_timestamp modules=$modules operation=$operation upgrade_what=$up_what"

    log "INFO: zk_included=$zk_included"
    log "INFO: hdfs_included=$hdfs_included"
    log "INFO: hbase_included=$hbase_included"

    log "ERROR: upgrade has not been supported yet!"
    exit 1
fi

log "INFO: Succeeded."
exit 0
