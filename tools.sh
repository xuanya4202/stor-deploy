#!/bin/bash

if [ -z "$tools_defined" ] ; then
tools_defined="true"

TOOLS="${BASH_SOURCE-$0}"
TOOLS_PDIR="$(dirname "${TOOLS}")"
TOOLS_PDIR="$(cd "${TOOLS_PDIR}"; pwd)"

###################### log-tool ######################

mkdir -p $TOOLS_PDIR/logs
LOG_FILE=$TOOLS_PDIR/logs/deploy.log

function log()
{
    local time_stamp=`date "+%Y-%m-%d %H:%M:%S"`

    echo "$time_stamp    $*" | tee -a $LOG_FILE
    return 0
}

fi
