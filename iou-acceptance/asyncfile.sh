#!/usr/bin/env bash
#!/usr/bin/env bash
#set -x
set -e

<<END_COMM
AsyncFileTest
END_COMM


FDBCLI="/mnt/ddi/uringdb/bld/bin/fdbcli"
FDBSERVER="/mnt/ddi/uringdb/bld/bin/fdbserver"
LIB="/mnt/nvme/nvme0/ddi/liburingsrc"
#use .stub for the stub and .txt for the test
TEST="/mnt/nvme/nvme0/ddi/uringdb/tests/IOU"
CLS="/home/ddi/fdb.flex13"
#device on which  the data and log path are mounted (used for io stat collection)
DEV="nvme0n1"
DATALOGPATH="/mnt/nvme/nvme0"


uring=""
uring_srv=""

run_test(){
    out=${1}
    uring=${2}
    #spawn the orchestrator
    #https://stackoverflow.com/questions/13356628/how-to-redirect-the-output-of-the-time-command-to-a-file-in-linux
    iostat -x 1 -p ${DEV} > io_$out &
    {  time LD_LIBRARY_PATH=${LIB} ${FDBSERVER}  -r test -f ${TEST}.txt -C ${CLS} --memory 64GB ${uring} ; } > ${out} 2>&1
}




spawn(){

    pkill -9 fdbserver || true    #if nothing is killed, error is returned
    sleep 1

    data_dir=${DATALOGPATH}/"tmp_dir"


    port=4500

    #remove the old test file
    fn=$(cat ${TEST}.txt | grep "fileName" | cut -d= -f2)
    rm ${fn} || true


    mkdir -p ${DATALOGPATH}
    rm -rf ${DATALOGPATH}/*
    #spawn one-process cluster
    mkdir ${data_dir}/${port} || true
    LD_LIBRARY_PATH=${LIB}  ${FDBSERVER} -C ${CLS} -p auto:${port} --listen_address public ${uring_srv}  --datadir=${data_dir}/${port} --logdir=${data_dir}/${port} &

    #spawn the test role
    port=$((${port}+1))
    mkdir ${data_dir}/${port} || true
    LD_LIBRARY_PATH=${LIB} ${FDBSERVER} -C ${CLS} -c test -p auto:${port} --listen_address public ${uring_srv} --datadir=${data_dir}/${port} --logdir=${data_dir}/${port} &

    sleep 5 #give time to join the cluster

    #create the db
    #LD_LIBRARY_PATH=${lb} ${cli} -C ${cls} --exec "configure new single ssd-2"
}

setup_test(){
    if [[ $1 == "io_uring" ]]; then
        uring="--knob_enable_io_uring true --knob_io_uring_direct_submit true"
        echo "URING"
    elif [[ $1 == "kaio" ]];then
        uring=""
        echo "KAIO"
    else
        echo "Mode not supported. Use either io_uring or kaio"
        exit 1
    fi

    uring_srv=${uring}

    cp ${TEST}.stub ${TEST}.txt
    sed -i  "s/TEST_DURATION/$2/g" ${TEST}.txt
    sed -i  "s/TEST_READS/$3/g" ${TEST}.txt
    sed -i  "s/TEST_UNBUFFERED/$4/g" ${TEST}.txt
    sed -i  "s/TEST_UNCACHED/$5/g" ${TEST}.txt
    sed -i  "s/TEST_WRITE_FRACTION/$6/g" ${TEST}.txt
}

run_one(){
    io=$1
    duration=$2
    parallel_reads=$3
    unbuffered=$4  #buffered/unbuffered
    uncached=$5  #true/false
    write_fraction=$6
    run=${7}

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

    spawn

    time run_test ${out_file} "${uring}"
    #cat ${timing} >> ${out_file}
    #kill server and iostat
    pkill -9 fdbserver
    pkill -9 iostat
}

sec=30
buff="unbuffered" #buffered unbuffered
cached="cached"   #cached uncached

for run in 1 2; do
    for parallel_reads in 64 32 1; do
        for write_perc in 0 50 100;do
            for io in "io_uring" "kaio"; do
                run_one ${io} ${sec} ${parallel_reads} ${buff} ${cached} ${write_perc} ${run}
            done #uring
        done #write perc
     done #reads
done #run


#comparing to
#sudo fio --filename=/mnt/nvme/nvme10/aftest.bin  --direct=1 --rw=randread --bs=4k --ioengine=libaio --iodepth=128 --runtime=30 --numjobs=20 --time_based --group_reporting --name=throughput-test-job --eta-newline=1 --readonly --size=10G
