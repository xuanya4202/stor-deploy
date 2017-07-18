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
conf_dir=
operation=
from=
to=
up_what=

zk_included=
hdfs_included=
hbase_included=

function usage()
{
    echo "Usage: $0 -m {modules} [-c {conf-dir}] -o {operation} [-f {from}] [-t {to}] [-u {upgrade-what}]"
    echo "    -m modules      : one or more modules in zk, hdfs and hbase, separated by comma"
    echo "                      such as 'zk' or 'zk,hdfs' or 'zk,hdfs,hbase'"
    echo "    -d conf-dir     : the directory containing stor.conf and stor-default.conf; by default, {conf-dir}=./conf"
    echo "    -o operation    : deploy | upgrade"
    echo "    -f from -t to   : only used when operation=deploy. deploy consists of multiple steps:"
    echo "                            parse       : parse the config files in {conf-dir}"
    echo "                            config      : generate config files for the seleted modules"
    echo "                            check       : basic environment check, such as if java is available"
    echo "                            stop        : stop existing processes"
    echo "                            uninstall   : remove old installation"
    echo "                            prepare     : do preparation work, such as mounting disks, make clean dirs"
    echo "                            install_pkg : dispatch software packages"
    echo "                            install_cfg : dispatch the config files"
    echo "                            run         : start up the new installation"
    echo "                            verify      : verify if all processes are running properly"
    echo "                      by default {from}=parse"
    echo "                      by default {to}=run"
    echo "    -u upgrade-what : only used when operation=upgrade"
    echo "                            conf    : upgrade the config files, and restart"
    echo "                            package : upgrade the softwarepackage and config files, and restart"
    echo "                      by default {upgrade-what}=conf"
}

while getopts "m:c:o:f:t:u:h" opt ; do
    case $opt in 
        m)
            modules=$OPTARG
            ;;
        c)
            conf_dir=$OPTARG
            ;;
        o)
            operation=$OPTARG
            ;;
        f)
            from=$OPTARG
            ;;
        t)
            to=$OPTARG
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

if [ -z "$modules" ] ; then
    echo "ERROR: argument 'modules' must be present!"
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

[ -z "$conf_dir" ] && conf_dir=$SCRIPT_PDIR/conf
if [ ! -d "$conf_dir" ] ; then
    echo "ERROR: {conf-dir} must be an existing directory"
    usage
    exit 1
fi


if [ ! -f $conf_dir/stor-default.conf -a ! -f $conf_dir/stor.conf ] ; then
    echo "ERROR: at least one of $conf_dir/stor-default.conf and $conf_dir/stor.conf is mandatory"
    usage
    exit 1
fi

if [ -z "$operation" ] ; then
    echo "ERROR: argument 'operation' must be present!"
    usage
    exit 1
fi

if [ X"$operation" == "Xdeploy" ] ; then
    steps=("parse" "config" "check" "stop" "uninstall" "prepare" "install_pkg" "install_cfg" "run" "verify")
    [ -z "$from" ] && from=parse
    [ -z "$to" ] && to=verify
    i=0
    n=${#steps[@]}
    found=0
    while [ $i -lt $n ] ; do
        [ "X$from" == "X${steps[$i]}" ] && from=$i && found=`expr $found + 1`
        [ "X$to" == "X${steps[$i]}" ] && to=$i && found=`expr $found + 1`
        i=`expr $i + 1`
    done

    if [ $found -ne 2 ] ; then
        echo "ERROR: argument 'from' or 'to' is invalid!"
        usage
        exit 1
    fi
elif [ X"$operation" == "Xupgrade" ] ; then
    if [ X"$up_what" != "Xconf" -a X"$up_what" != "Xpackage" ] ; then
        echo "ERROR: argument 'upgrade-what' is invalid!"
        usage
        exit 1
    fi
else
    echo "ERROR: argument 'operation' is invalid: it must be 'deploy' or 'upgrade'"
    usage
    exit 1
fi

log " "
log " "
log "INFO: ========================================== `basename $0` =========================================="


parsed_conf=$LOGS/parsed-config

if [ X"$operation" = "Xdeploy" ] ; then
    log "INFO: run_timestamp=$run_timestamp modules=$modules operation=$operation from=$from to=$to"

    log "INFO: zk_included=$zk_included"
    log "INFO: hdfs_included=$hdfs_included"
    log "INFO: hbase_included=$hbase_included"

    if [ $to -ge 3 ] ; then  #stop or later
        log "operation=$operation, data will be cleared, are you sure? [yes/no]"
        read answer
        if [ X"$answer" != "Xyes" ] ; then
            log "Your answer ($answer) is not 'yes', exit!"
            exit 0
        else
            log "Your answer is 'yes', continue!"
        fi
    fi

    if [ "X$zk_included" = "Xtrue" ] ; then
        if [ $from -le 0 -a $to -ge 0 ] ; then
            rm -fr $parsed_conf/zk || return 1
            mkdir -p $parsed_conf/zk || return 1

            parse_configuration $conf_dir/stor-default.conf $conf_dir/stor.conf $parsed_conf "$modules"
            if [ $? -ne 0 ] ; then
                log "ERROR: parse_configuration failed"
                exit 1
            fi
        fi

        if [ $to -ge 1 ] ; then
            deploy_zk "$parsed_conf" $from $to
            if [ $? -ne 0 ] ; then
                log "ERROR: deploy_zk failed"
                exit 1
            fi
        fi
    fi
    
    if [ "X$hdfs_included" = "Xtrue" ] ; then
        if [ $from -le 0 -a $to -ge 0 ] ; then
            rm -fr $parsed_conf/hdfs || return 1
            mkdir -p $parsed_conf/hdfs || return 1

            parse_configuration $conf_dir/stor-default.conf $conf_dir/stor.conf $parsed_conf "$modules"
            if [ $? -ne 0 ] ; then
                log "ERROR: parse_configuration failed"
                exit 1
            fi
        fi

        if [ $to -ge 1 ] ; then
            deploy_hdfs "$parsed_conf" $from $to "$zk_included"
            if [ $? -ne 0 ] ; then
                log "ERROR: deploy_hdfs failed"
                exit 1
            fi
        fi
    fi
    
    if [ "X$hbase_included" = "Xtrue" ] ; then
        if [ $from -le 0 -a $to -ge 0 ] ; then
            rm -fr $parsed_conf/hbase || return 1
            mkdir -p $parsed_conf/hbase || return 1

            parse_configuration $conf_dir/stor-default.conf $conf_dir/stor.conf $parsed_conf "$modules"
            if [ $? -ne 0 ] ; then
                log "ERROR: parse_configuration failed"
                exit 1
            fi
        fi

        if [ $to -ge 1 ] ; then
            deploy_hbase "$parsed_conf" $from $to "$zk_included"
            if [ $? -ne 0 ] ; then
                log "ERROR: deploy_hbase failed"
                exit 1
            fi
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
