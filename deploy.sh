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

run_timestamp=`date +%Y%m%d%H%M%S`

modules=
operation=

zk_included=
hdfs_included=
hbase_included=

function usage()
{
    echo "Usage: $0 -m {modules} -o {operation}"
    echo "    modules   : one or more modules in zk, hdfs and hbase, separated by comma"
    echo "                such as 'zk' or 'zk,hdfs' or 'zk,hdfs,hbase'"
    echo "    operation : parse | check | clean | prepare | install | deploy"
    echo "                deploy - completely remove original data and software, and re-deploy from scratch"
    echo "                         so, You Must Be Very Very Careful !!!"
}

while getopts "m:o:h" opt ; do
    case $opt in 
        m)
            modules=$OPTARG
            ;;
        o)
            operation=$OPTARG
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


if [ -z "$modules" ] ; then
    echo "ERROR: argument 'modules' is missing."
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

if [ -z "$operation" ] ; then
    echo "ERROR: argument 'operation' is missing."
    usage
    exit 1
fi

log ""
log "INFO: =============================== `basename $0` ==============================="
log "INFO: run_timestamp=$run_timestamp modules=$modules operation=$operation"


if [ "X$operation" = "Xclean" -o "X$operation" = "Xprepare" -o "X$operation" = "Xinstall" -o "X$operation" = "Xdeploy" ] ; then
    log "Operation is \"$operation\" so data will be cleared, are you sure? [yes/no]"
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

log "INFO: zk_included=$zk_included"
log "INFO: hdfs_included=$hdfs_included"
log "INFO: hbase_included=$hbase_included"

if [ "X$zk_included" = "Xtrue" ] ; then
    deploy_zk "$LOGS/deploy-$run_timestamp" "$operation"
    if [ $? -ne 0 ] ; then
        log "ERROR: deploy_zk failed"
        exit 1
    fi
fi

if [ "X$hdfs_included" = "Xtrue" ] ; then
    deploy_hdfs "$LOGS/deploy-$run_timestamp" "$operation"
    if [ $? -ne 0 ] ; then
        log "ERROR: deploy_hdfs failed"
        exit 1
    fi
fi

log "INFO: Succeeded."
exit 0
