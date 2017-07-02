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
    log "INFO:       device=$device"
    log "INFO:       mntpoint=$mntpoint"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.umount_dev`

    $SSH umount $device > $sshErr 2>&1
    if [ -s $sshErr ] ; then
        cat $sshErr | grep "not mounted" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then   # didn't found 'not mounted'
            log "ERROR: Exit umount_dev(): failed to umount $device. See $sshErr for details"
            return 1
        fi
    fi

    $SSH umount $mntpoint > $sshErr 2>&1
    if [ -s $sshErr ] ; then
        cat $sshErr | grep -e "not mounted" -e "mountpoint not found" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then   # didn't found 'not mounted'
            log "ERROR: Exit umount_dev(): failed to umount $mntpoint. See $sshErr for details"
            return 1
        fi
    fi

    #remove the device from /fstab
    $SSH sed -i -e \'"\#$device#d"\' /etc/fstab 2> $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit umount_dev(): failed to remove $device from fstab. See $sshErr for details"
        return 1
    fi

    #remove the uuid of the device from /etc/fstab
    local uuid=`$SSH blkid $device 2> $sshErr | grep -w UUID | sed -e 's/.*\<UUID\>="\([-0-9a-fA-F]*\)".*/\1/'`
    log "INFO: in umount_dev(): uuid of $device is \"$uuid\""
    if [ -s $sshErr ] ; then
        log "ERROR: Exit umount_dev(): failed to get uuid of $device on $node. See $sshErr for details"
        return 1
    fi

    if [ -n "$uuid" ] ; then
        $SSH sed -i -e "/$uuid/d" /etc/fstab 2> $sshErr
        if [ -s $sshErr ] ; then
            log "ERROR: Exit umount_dev(): failed to remove uuid of $device from fstab. See $sshErr for details"
            return 1
        fi
    fi

    rm -f $sshErr

    log "INFO: Exit umount_dev(): Success"
    return 0
}

function format_dev()
{
    local node=$1
    local user=$2
    local ssh_port=$3
    local device=$4
    local mkfs_cmd=$5

    log "INFO: Enter format_dev(): node=$node user=$user ssh_port=$ssh_port"
    log "INFO:       device=$device"
    log "INFO:       mkfs_cmd=$mkfs_cmd"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.format_dev`

    $SSH $mkfs_cmd $device 2> $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit format_dev(): failed to format $device on $node. See $sshErr for details"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit format_dev(): Success"
    return 0
}

function mount_dev()
{
    local node=$1
    local user=$2
    local ssh_port=$3
    local device=$4
    local mntpoint=$5
    local fs_type=$6
    local mount_opts=$7

    log "INFO: Enter mount_dev(): node=$node user=$user ssh_port=$ssh_port"
    log "INFO:       device=$device"
    log "INFO:       mntpoint=$mntpoint"
    log "INFO:       fs_type=$fs_type"
    log "INFO:       mount_opts=$mount_opts"

    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.mount_dev`

    $SSH mkdir -p $mntpoint 2> $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit mount_dev(): failed to create mount point $mntpoint on $node. See $sshErr for details"
        return 1
    fi

    $SSH mount -t $fs_type $device $mntpoint -o $mount_opts 2> $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit mount_dev(): failed to mount $device on $mntpoint on $node. See $sshErr for details"
        return 1
    fi

    #add the mount in /etc/fstab, by uuid.
    local uuid=`$SSH blkid $device 2> $sshErr | grep -w UUID | sed -e 's/.*\<UUID\>="\([-0-9a-fA-F]*\)".*/\1/'`
    log "INFO: in mount_dev(): uuid of $device is \"$uuid\""
    if [ -s $sshErr ] ; then
        log "ERROR: Exit mount_dev(): failed to get uuid of $device on $node. See $sshErr for details"
        return 1
    fi

    if [ -z "$uuid" ] ; then
        log "ERROR: Exit mount_dev(): uuid of $device is empty"
        return 1
    fi

    #try to remove original mount;
    $SSH sed -i -e "/$uuid/d" /etc/fstab 2> $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit mount_dev(): failed to remove uuid of $device from fstab. See $sshErr for details"
        return 1
    fi

    #add the mount;
    local new_mnt="UUID=$uuid    $mntpoint    $fs_type    $mount_opts    0    0"
    $SSH sed -i -e \'"$ a $new_mnt"\' /etc/fstab 2> $sshErr
    if [ -s $sshErr ] ; then
        log "ERROR: Exit mount_dev(): failed to add mount into fstab. See $sshErr for details"
        return 1
    fi

    rm -f $sshErr

    log "INFO: Exit mount_dev(): Success"
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
    log "INFO:       mounts=$mounts"
    log "INFO:       mount_opts=$mount_opts"
    log "INFO:       mkfs_cmd=$mkfs_cmd"

    local fs_type=""

    #Yuanguo: for now, only mkfs.ext4 and mkfs.xfs are supported 
    echo "$mkfs_cmd" | grep "mkfs.ext4" > /dev/null 2>&1
    if [ $? -eq 0 ] ; then   #ext4
        mkfs_cmd="$mkfs_cmd -F -q"
        fs_type="ext4"
    else
        echo "$mkfs_cmd" | grep "mkfs.xfs" > /dev/null 2>&1
        if [ $? -eq 0 ] ; then   #xfs
            mkfs_cmd="$mkfs_cmd -f"
            fs_type="xfs"
        else
            log "ERROR: Exit do_mounts(): only mkfs.ext4 and mkfs.xfs are supported for now. mkfs_cmd=$mkfs_cmd"
            return 1
        fi
    fi


    #get the current mounts of $node, so that we can check if our requested device are already mounted on the requested
    #mount point;
    local SSH="ssh -p $ssh_port $user@$node"
    local sshErr=`mktemp --suffix=-stor-deploy.do_mounts`
    local node_mounts=`mktemp --suffix=-stor-deploy.node_mounts`
    $SSH mount > $node_mounts 2> $sshErr
    if [ -s $sshErr -o ! -s $node_mounts ] ; then
        log "ERROR: Exit do_mounts(): failed to get mounts of $node. See $sshErr and $node_mounts for details"
        return 1
    fi

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

        log "INFO: in do_mounts(): got a mount config: $one_mnt"

        local device=`echo $one_mnt | cut -d ':' -f 1`
        local mntpoint=`echo $one_mnt | cut -d ':' -f 2`

        device=`echo $device | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`
        mntpoint=`echo $mntpoint | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'`

        if [ -z "$device" -o -z "$mntpoint" ] ; then
            log "ERROR: Exit do_mounts(): mounts is invalid: mounts=$mounts device=$device mntpoint=$mntpoint"
            return 1
        fi

        local old_mntpoint=`grep "$device" $node_mounts | cut -d ' ' -f 3`
        if [ -z "$old_mntpoint" ] ; then
            log "INFO: in do_mounts(): $device is not mounted on $node, we need to mount it on $mntpoint"
        elif [ "$old_mntpoint" != "$mntpoint" ] ; then
            log "INFO: in do_mounts(): $device is mounted on $old_mntpoint on $node, we need to re-mount it on $mntpoint"
        else
            log "WARN: in do_mounts(): $device is already mounted on $mntpoint on $node, we need not to re-mount it"
            continue
        fi

        umount_dev "$node" "$user" "$ssh_port" "$device" "$mntpoint"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit do_mounts(): umount_dev failed. node=$node device=$device mntpoint=$mntpoint"
            return 1
        fi

        format_dev "$node" "$user" "$ssh_port" "$device" "$mkfs_cmd"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit do_mounts(): format_dev failed. node=$node device=$device mkfs_cmd=$mkfs_cmd"
            return 1
        fi

        mount_dev "$node" "$user" "$ssh_port" "$device" "$mntpoint" "$fs_type" "$mount_opts"
        if [ $? -ne 0 ] ; then
            log "ERROR: Exit do_mounts(): mount_dev failed. node=$node device=$device mntpoint=$mntpoint mount_opts=$mount_opts"
            return 1
        fi
    done

    rm -f $sshErr $node_mounts

    log "INFO: Exit do_mounts(): Success"
    return 0
}

fi
