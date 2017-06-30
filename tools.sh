#!/bin/bash

if [ -z "$tools_defined" ] ; then
tools_defined="true"


###################### log-tool ######################

function log()
{
    [ -z "$LOG_FILE" ] && LOG_FILE=deploy.log

    local time_stamp=`date "+%Y-%m-%d %H:%M:%S"`
    echo "$time_stamp    $*" | tee -a $LOG_FILE
    return 0
}

##################### mount-tool #####################




fi
