FDBCLI="/mnt/ddi/uringdb/bld/bin/fdbcli"
FDBSERVER="/mnt/ddi/uringdb/bld/bin/fdbserver"
LIB="/mnt/ddi/uringdb/liburing/src"
#use .stub for the stub and .txt for the test
TEST="/mnt/ddi/uringdb/tests/IOU"
CLS="/home/ddi/fdb-official/fdb.zac13"
#device on which  the data and log path are mounted (used for io stat collection)
MOUNT_POINT="/mnt/nvme/nvme0"
DEV="nvme0n1"
DATALOGPATH=${MOUNT_POINT}"/ioutest"
FILEPATH=${MOUNT_POINT}"/testfiles"
PAGE_CACHE="10"  #MiB
RESULTS=`date +%Y-%m-%d_%H-%M-%S`
hn=$(hostname)
RESULTS="${RESULTS}-${hn}-ASYNC"
