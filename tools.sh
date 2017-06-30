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

function umount_dev()
{
    local node=$1
    local user=$2
    local ssh_port=$3
    local device=$4
    local mntpoint=$5

    log "INFO: Enter umount_dev(): node=$node user=$user ssh_port=$ssh_port"
    log "INFO:     device=$device"
    log "INFO:     mntpoint=$mntpoint"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.umount`

    $SSH umount $device > $sshErr 2>&1
    if [ -s $sshErr ] ; then
        cat $sshErr | grep "not mounted" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then   # didn't found 'not mounted'
            log "ERROR: Exit umount_dev(): failed to umount $device. See $sshErr for details"
            return 1
        fi
    fi

    $SSH umount $mntpoint  > $sshErr 2>&1 
    if [ -s $sshErr ] ; then
        cat $sshErr | grep "not mounted" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then   # didn't found 'not mounted'
            log "ERROR: Exit umount_dev(): failed to umount $mntpoint See $sshErr for details"
            return 1
        fi
    fi

    #remove the device from /fstab
    $SSH sed -i -e "\#$device#d" /etc/fstab 2> $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit umount_dev(): failed to remove $device from fstab. See $sshErr for details"
        return 1
    fi

    #remove the uuid of the device from /fstab
    local uuid=`$SSH blkid $device 2> $sshErr | grep -w UUID | sed -e 's/.*\<UUID\>="\([-0-9a-fA-F]*\)".*/\1/'`
    if [ -s $sshErr ] ; then
        log "ERROR: Exit umount_dev(): failed to get uuid of $device on $node. See $sshErr for details"
        return 1
    fi

    if [ -n "$uuid" ] ; then
        $SSH sed -i -e "/$uuid/d" /etc/fstab 2> $sshErr
        if [ -s $sshErr ] ; then
            log "ERROR: Exit umount_dev(): failed to remove uuid ($uuid) of $device from fstab. See $sshErr for details"
            return 1
        fi
    fi

    rm -f $sshErr

    log "INFO: Exit umount_dev(): Success"
    return 0
}

function do_mounts()
{
    local node=$1
    local user=$2
    local ssh_port=$3
    local mounts=$4
    local mount_opts=$5
    local mkfs_cmd=$6

    log "INFO: Enter do_mounts(): node=$node user=$user ssh_port=$ssh_port"
    log "INFO:     mounts=$mounts"
    log "INFO:     mount_opts=$mount_opts"
    log "INFO:     mkfs_cmd=$mkfs_cmd"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.domounts`

    while [ -n "$mounts" ] ; do
        local one_mnt=""

        echo "$mounts" | grep "," > /dev/null 2>&1
        if [ $? -eq 0 ] ; then     #',' is found
            one_mnt=`echo $mounts | cut -d ',' -f 1`
            mounts=`echo $mounts | cut -d ',' -f 2-`

            one_mnt=`echo $one_mnt | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
            mounts=`echo $mounts | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
        else
            one_mnt=$mounts
            mounts=""
        fi

        local device=`echo $one_mnt | cut -d ':' -f 1`
        local mntpoint=`echo $one_mnt | cut -d ':' -f 2`

        device=`echo $device | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
        mntpoint=`echo $mntpoint | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`

        if [ -z "$device" -o -z "$mntpoint" ] ; then
            log "ERROR: Exit do_mounts(): mounts is invalid: mounts=$mounts device=$device mntpoint=$mntpoint"
            return 1
        fi

    done

    rm -f $sshErr

    log "INFO: Exit do_mounts(): Success"
    return 0
}


umount_dev 192.168.100.132 root 22 /dev/sdd1 /home/watermelon



fi
