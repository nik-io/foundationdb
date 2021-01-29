set -e
FDBCLI="/mnt/nvme/nvme0/uringdb/bld/bin/fdbcli"
FDBSERVER="/mnt/nvme/nvme0/uringdb/bld/bin/fdbserver"
LIB="/mnt/nvme/nvme0/uringdb/liburing/src"
#use .stub for the stub and .txt for the test
TEST="/mnt/nvme/nvme0/uringdb/tests/RW"
CLS="/home/ddi/fdb-official/fdb.cluster"
FILEPATH="/mnt/nvme/nvme1/testfiles"
PAGE_CACHE="100"  #MiB
RESULTS=`date +%Y-%m-%d_%H-%M-%S`
hn=$(hostname)
RESULTS="${RESULTS}-${hn}-KV"
RESULTS="aaa"

DEVS=("/dev/nvme3n1" "/dev/nvme4n1" "/dev/nvme5n1" "/dev/nvme6n1" "dev/nvme7n1")
MNTS=("/mnt/nvme/nvme3" "/mnt/nvme/nvme4" "/mnt/nvme/nvme5" "/mnt/nvme/nvme6" "/mnt/nvme/nvme7")
USERGROUP="ddi:sto"

STORAGE_PER_DISK=3
LOG_PER_DISK=3
STORAGE_DISKS=3
LOG_DISKS=3

STORAGES=$(( STORAGE_PER_DISK * STORAGE_DISKS ))
LOGS=$(( LOG_PER_DISK * LOG_DISKS))

if [[ $(( $STORAGE_DISKS + $LOG_DISKS + 1 )) > ${#DEVS[@]} ]] || [[ ${#DEVS[@]} != ${#MNTS[@]} ]];then
	echo "error"
	exit 1
fi
TRIM=0

trim(){
	if [[ $TRIM == 1 ]];then
		#Keepalive for sudo. https://gist.github.com/cowboy/3118588
		# Might as well ask for password up-front, right?
		sudo -v
		# Keep-alive: update existing sudo time stamp if set, otherwise do nothing.
		while true; do
			sudo -n true
			sleep 60
			kill -0 "$$" || exit
		done 2>/dev/null &

		for i in "${!DEVS[@]}"; do 
			sudo mount | grep -qs ${DEVS[$i]}
			ret=$?
			if [ $ret -eq 0 ];then
				echo "umounting ${DEVS[$i]}"
				sudo umount ${MNTS[$i]}
				ret=$?
				if [[ $ret -ne 0 ]]; then
					echo "umount ${MNTTS[$1]} failed with ret $ret"
					exit 1
				fi
			fi

			echo "Trimming ${DEV[$1]}"
			sudo /sbin/blkdiscard ${DEVS[$1]}
			yes | sudo mkfs.ext4 ${DEVS[$i]} -E lazy_itable_init=0,lazy_journal_init=0,nodiscard
			if [[ $? -ne 0 ]]; then
				echo "ext4 failed"
				exit 1
			fi
			sudo mount ${DEVS[$i]} ${MNTS[$i]}
			if [[ $? -ne 0 ]]; then
				echo "mount ${MNTS[$i]} failed"
				exit 1
			fi
			sudo chown -R $USERGROUP ${MNTS[$i]}
		done
	fi
}






