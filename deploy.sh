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

run_timestamp=`date +%Y%m%d%H%M%S`

modules=
operation=

function usage()
{
    echo "Usage: $0 -m {modules} -o {operation}"
    echo "    modules   : one or more modules in zk, hdfs and hbase, separated by comma"
    echo "                such as 'zk' or 'zk,hdfs' or 'zk,hdfs,hbase'"
    echo "    operation : deploy | update"
    echo "                deploy - completely remove original data and software, and re-deploy from scratch"
    echo "                         so, You Must Be Very Very Careful !!!"
    echo "                update - update the configuration files based on stor.conf and stor-default.conf"
    echo "                         and restart the modules"
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
    if [ "$m" != "zk" -a  "$m" != "hdfs"  -a "$m" != "hbase" ] ; then
        echo "ERROR: argument 'modules' is invalid: $m is unrecognized, it must be 'zk', 'hdfs' or 'hbase'"
        usage
        exit 1
    fi
done

if [ -z "$operation" ] ; then
    echo "ERROR: argument 'operation' is missing."
    usage
    exit 1
fi

if [ "$operation" != "deploy" -a "$operation" != "update" ] ; then
        echo "ERROR: argument 'operation' is invalid: $operation is unrecognized, it must be 'deploy' or 'update'"
        usage
        exit 1
fi

log ""
log "INFO: =============================== `basename $0` ==============================="
log "INFO: run_timestamp=$run_timestamp modules=$modules operation=$operation"


if [ "$operation" = "deploy" ] ; then
    log "Operation is 'deploy' so data will be cleared, are you sure? [yes/no]"
    read answer
    if [ X"$answer" != "Xyes" ] ; then
        log "Your answer ($answer) is not 'yes', exit!"
        exit 0
    else
        log "Your answer is 'yes', continue!"
    fi
fi

#TODO: checks;

parse_configuration $SCRIPT_PDIR/stor-default.conf $SCRIPT_PDIR/stor.conf $LOGS/deploy-$run_timestamp "$modules"
if [ $? -ne 0 ] ; then
    echo "ERROR: parse_configuration failed"
    exit 1
fi

deploy_zk $LOGS/deploy-$run_timestamp
if [ $? -ne 0 ] ; then
    echo "ERROR: deploy_zk failed"
    exit 1
fi

echo "INFO: Succeeded."
exit 0
