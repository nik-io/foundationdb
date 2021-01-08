#!/usr/bin/env bash
#!/usr/bin/env bash
#set -x
#set -e

<<END_COMM
AsyncFileTest
END_COMM

FDBCLI="/mnt/ddi/uringdb/bld/bin/fdbcli"
FDBSERVER="/mnt/ddi/uringdb/bld/bin/fdbserver"
LIB="/mnt/ddi/uringdb/liburing/src"
#use .stub for the stub and .txt for the test
TEST="/mnt/ddi/uringdb/tests/IOU"
CLS="/home/ddi/fdb-official/fdb.zac13"
#device on which  the data and log path are mounted (used for io stat collection)
MOUNT_POINT="/mnt/nvme/nvme0/"
DEV="nvme0n1"
DATALOGPATH=${MOUNT_POINT}"ioutest"
FILEPATH=${MOUNT_POINT}"testfiles"
PAGE_CACHE="10"  #MiB
RESULTS=`date +%Y-%m-%d_%H-%M-%S`
hn=$(hostname)
RESULTS="${RESULTS}-${hn}-ASYNC"
RESULTS="ttestt5"
mkdir -p ${RESULTS} || exit 1
port=

CORE=
testpid=
testport=

uring=""

TRIM=1
#If TRIM is enabled, the file has to be retrieved from somewhere to avoid creating it every time
#Be sure that the name of the file matches the field in the test file
PRE_TEST_FILE="/mnt/ddi/file.dat"
USERGROUP="ddi:sto"

if [[ $TRIM == 1 ]]; then
	#Keepalive for sudo. https://gist.github.com/cowboy/3118588
	# Might as well ask for password up-front, right?
	sudo -v

    # Keep-alive: update existing sudo time stamp if set, otherwise do nothing.
    while true; do
	    sudo -n true
	    sleep 60
	    kill -0 "$$" || exit
    done 2>/dev/null &
fi


run_test(){
	out=${1}
	uring=${2}
	mem="4GB"
	mkdir -p ${data_dir}/${port} || true
	#spawn the orchestrator
	#https://stackoverflow.com/questions/13356628/how-to-redirect-the-output-of-the-time-command-to-a-file-in-linux
	iostat -x 1 -p ${DEV} > ${RESULTS}/iostat_$out &

	{ time LD_LIBRARY_PATH=${LIB} taskset -c ${CORE} ${FDBSERVER}  -r multitest -f ${TEST}.txt -C ${CLS} --memory ${mem} ${uring} --logdir=${DATALOGPATH} ;}  > ${RESULTS}/${out} 2>&1 &
		#LD_LIBRARY_PATH=${LIB} gdb -ex run --args  ${FDBSERVER}  -r test -f ${TEST}.txt -C ${CLS} --memory ${mem} ${uring} --logdir=${DATALOGPATH}
		#Take the pid of the orchestrator by taking the pid of "time" and pgrepping by parent
		timepid=$!
		orchpid=$(pgrep -P $timepid)
		echo "orch pid ${orchpid}"
		CORE=$(( $CORE + 1 ))
		if [[ -z $orchpid ]];then
			exit 1  #we exit so that we can replicate the last orchestrator command and see what is what
		fi
		while kill -0 $orchpid ; do pmap $testpid | grep total | awk '{print $2}' >> ${RESULTS}/pmap_$out ; sleep 1 ;done
	}


spawn(){
	pkill -9 fdbserver || true    #if nothing is killed, error is returned
	pkill -9 iostat || true
	pkill -9 pstat || true

	sleep 1

	data_dir=${DATALOGPATH}



	#remove the old test file
	fn=$(cat ${TEST}.txt | grep "fileName" | cut -d= -f2)
	#echo "removing ${fn}"
	#rm ${fn} || true

#	if [[ $TRIM == 1 ]];then
#		echo "Copying $fn to $PRE_TEST_FILE"
#		cp $PRE_TEST_FILE $fn
#	fi


mkdir -p ${DATALOGPATH}
echo "removing ${DATALOGPATH}/*"
rm -rf ${DATALOGPATH}/*
#spawn one-process cluster
mkdir -p ${data_dir}/${port} || true
LD_LIBRARY_PATH=${LIB}  taskset -c ${CORE} ${FDBSERVER} -C ${CLS} -p auto:${port} --listen_address public ${uring}  --datadir=${data_dir}/${port} --logdir=${data_dir}/${port} &
CORE=$(( $CORE + 1 ))

	#spawn the test role
	port=$((${port}+1))
	mkdir ${data_dir}/${port} || true
	LD_LIBRARY_PATH=${LIB} taskset -c ${CORE} ${FDBSERVER} -C ${CLS} -c test -p auto:${port} --listen_address public ${uring} --datadir=${data_dir}/${port} --logdir=${data_dir}/${port} &
	testpid=$!
	testport=${port}
	echo "Test pid is $testpid"
	CORE=$(( $CORE + 1 ))

	sleep 5 #give time to join the cluster

    #create the db
    #LD_LIBRARY_PATH=${lb} ${cli} -C ${cls} --exec "configure new single ssd-2"
}

setup_test(){
	if [[ $TRIM == 1 ]];then
		echo "Trimming /dev/$DEV"
		sudo umount $MOUNT_POINT || true
		if [[ $? -ne 0 ]]; then
			echo "umount ${MOUNT_POINT} failed"
			exit 1
		fi
		sudo /sbin/blkdiscard /dev/$DEV
		yes | sudo mkfs.ext4 /dev/$DEV -E nodiscard
		if [[ $? -ne 0 ]]; then
			echo "ext4 failed"
			exit 1
		fi
	fi
	sudo mount /dev/$DEV $MOUNT_POINT
	if [[ $? -ne 0 ]]; then
		echo "mount ${MOUNT_POINT} failed"
		exit 1
	fi

	sudo chown -R $USERGROUP ${MOUNT_POINT}


	pc=$(( ${PAGE_CACHE} * 1024 * 1024 ))
	if [[ $1 == "io_uring" ]]; then
		uring="--knob_enable_io_uring true --knob_io_uring_direct_submit true --knob_page_cache_4k ${pc}"
		echo "URING"
	elif [[ $1 == "kaio" ]];then
		uring=" --knob_page_cache_4k ${pc}"
		echo "KAIO"
	else
		echo "Mode not supported. Use either io_uring or kaio"
		exit 1
	fi

	mkdir -p $FILEPATH
	cp ${TEST}.stub ${TEST}.txt
	sed -i  "s/TEST_DURATION/$2/g" ${TEST}.txt
	sed -i  "s/TEST_READS/$3/g" ${TEST}.txt
	sed -i  "s/TEST_UNBUFFERED/$4/g" ${TEST}.txt
	sed -i  "s/TEST_UNCACHED/$5/g" ${TEST}.txt
	sed -i  "s/TEST_WRITE_FRACTION/$6/g" ${TEST}.txt
	#replace slash in path with escaped slash
	#https://unix.stackexchange.com/questions/211834/slash-and-backslash-in-sed
	file=$(echo "${FILEPATH}/file.dat" |  sed -e 's/\//\\\//g')
	sed -i  "s/FILE_NAME/${file}/g" ${TEST}.txt
}

run_one(){
	io=$1
	duration=$2
	parallel_reads=$3
	unbuffered=$4  #buffered/unbuffered
	uncached=$5  #true/false
	write_fraction=$6
	run=${7}
	CORE=1
	port=4500

	out_file="io=${io}_s=${duration}_pr=${parallel_reads}_b=${unbuffered}_c=${uncached}_w=${write_fraction}_r=${run}.txt"
	echo ${out_file}
	if [[ $5 == "cached" ]];then
		uncached="false"
	else
		uncached="true"
	fi

	if [[ $4 == "buffered" ]];then
		unbuffered="false"
	else
		unbuffered="true"
	fi

	setup_test $io $duration $parallel_reads $unbuffered $uncached $write_fraction
	cp ${TEST}.txt $RESULTS/TEST_$out_file

	spawn

	time run_test ${out_file} "${uring}"
	#cat ${timing} >> ${out_file}
	#kill server and iostat
	pkill -9 fdbserver
	pkill -9 iostat

	#copy the xml file of the test server 
	xml=$(ls ${DATALOGPATH}/${testport}/*xml | tail -n1)
	cp $xml $RESULTS/$out_file.xml
}

sec=60
buff="unbuffered" #buffered unbuffered
cached="uncached"   #cached uncached

for b in "unbuffered"; do
	for c in "uncached";do
		for run in 1 2 3 4 5; do
			for parallel_reads in 64; do
				for write_perc in 0;do
					for io in  "io_uring" "kaio"; do
						run_one ${io} ${sec} ${parallel_reads} ${b} ${c} ${write_perc} ${run}
					done #uring
				done #write perc
			done #reads
		done #run
	done
done


#comparing to
#sudo fio --filename=/mnt/nvme/nvme10/aftest.bin  --direct=1 --rw=randread --bs=4k --ioengine=libaio --iodepth=128 --runtime=30 --numjobs=20 --time_based --group_reporting --name=throughput-test-job --eta-newline=1 --readonly --size=10G
