#include "FastAlloc.h"
#include "FastRef.h"
#include "Knobs.h"
#include "Platform.h"
#include "Trace.h"
#include "flow.h"
#include "genericactors.actor.h"
#include "liburing/include/liburing.h"
#include <algorithm>
#include <asm-generic/errno-base.h>
#include <asm-generic/errno.h>
#include <asm-generic/socket.h>
#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>
#include <cmath>
#include <cstring>
#include <netinet/in.h>
#include <queue>
#include <sys/eventfd.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <unistd.h>
#define BOOST_SYSTEM_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#define BOOST_REGEX_NO_LIB
#include "boost/asio.hpp"
#include "boost/bind.hpp"
#include "boost/date_time/posix_time/posix_time_types.hpp"
#include "actorcompiler.h"
#include "network.h"
#include "FailureMonitor.h"
#include "IThreadPool.h"
#include "boost/range.hpp"
#include "IAsyncFile.h"
#include "UnitTest.h"
#if defined(__unixish__)
#if defined(__linux__)
#include <sys/types.h>
#endif
#include <signal.h>
#endif

extern void* getCurrentCoro();
extern void startProfiling(INetwork* network);

#include "AsyncFileEIO.actor.h"
#include "AsyncFileWinASIO.actor.h"
#include "AsyncFileKAIO.actor.h"
#include "AsyncFileIOUring.actor.h"
#include "AsyncFileCached.actor.h"
#include "libcoroutine/Coro.h"
#include "ActorCollection.h"
#include "ThreadSafeQueue.h"
#include "ThreadHelper.actor.h"
#include "simulator.h"
#include "Stats.h"

#ifdef WIN32
#include <mmsystem.h>
#endif
#include <atomic>

using namespace boost::asio::ip;

#undef FLOW_ACOMPILER_STATE
#define FLOW_ACOMPILER_STATE 1

#define SLOW_TASK_PROFILE 0
#if defined(__linux__) && SLOW_TASK_PROFILE
int net2liveness = 0;

size_t net2backtraces_max = 10000;
void** net2backtraces = NULL;
size_t net2backtraces_offset = 0;
bool net2backtraces_overflow = false;
int net2backtraces_count = 0;
#endif

namespace N2 { // No indent, it's the whole file

class Net2;
class Peer;
class Connection;

Net2* g_net2 = 0;

class Task
{
public:
    virtual void operator()() = 0;
};

class IReactor
{
public:
    virtual void sleepAndReact(double timeout) = 0;
    virtual void wake() = 0;
    virtual void addref() = 0;
    virtual void delref() = 0;
};

class ASIOReactor : public IReactor,
                    public FastAllocated<ASIOReactor>,
                    ReferenceCounted<ASIOReactor>
{
public:
    explicit ASIOReactor(Net2*);

    void sleepAndReact(double timeout) override;

    void wake() override;

    void addref() override { ReferenceCounted<ASIOReactor>::addref(); }
    void delref() override { ReferenceCounted<ASIOReactor>::delref(); }

    boost::asio::io_service ios;
    boost::asio::io_service::work do_not_stop; // Reactor needs to keep running
                                               // when there is nothing to do
                                               // until stopped explicitly

#ifdef __linux__
    Reference<IEventFD> createEventFD()
    {
        return Reference<IEventFD>(new EventFD(this));
    }
#endif

private:
    Net2* network;
    boost::asio::deadline_timer firstTimer;

    static void nullWaitHandler(const boost::system::error_code&) {}
    static void nullCompletionHandler() {}

#ifdef __linux__
    class EventFD : public IEventFD
    {
        int fd;
        ASIOReactor* reactor;
        boost::asio::posix::stream_descriptor sd;
        int64_t fdVal;

        static void handle_read(Promise<int64_t> p, int64_t* pVal,
                                const boost::system::error_code& ec,
                                std::size_t bytes_transferred)
        {
            if (ec)
                return; // Presumably, the EventFD was destroyed?
            ASSERT(bytes_transferred == sizeof(*pVal));
            p.send(*pVal);
        }

    public:
        EventFD(ASIOReactor* reactor)
          : reactor(reactor)
          , sd(reactor->ios, open())
        {}
        ~EventFD() override
        {
            sd.close(); // Also closes the fd, I assume...
        }
        int getFD() override { return fd; }
        Future<int64_t> read() override
        {
            Promise<int64_t> p;
            sd.async_read_some(
                boost::asio::mutable_buffers_1(&fdVal, sizeof(fdVal)),
                boost::bind(&EventFD::handle_read, p, &fdVal,
                            boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred));
            return p.getFuture();
        }

        int open()
        {
            fd = eventfd(0, EFD_NONBLOCK);
            if (fd < 0) {
                TraceEvent(SevError, "eventfdError").GetLastError();
                throw platform_error();
            }
            return fd;
        }
    };
#endif
};

thread_local INetwork* thread_network = 0;
Coro *current_coro = 0, *main_coro = 0;
Coro*
swapCoro(Coro* n)
{
    Coro* t = current_coro;
    current_coro = n;
    return t;
}

class Net2 sealed : public INetwork, public INetworkConnections
{
public:
    Net2(NetworkAddress localAddress, bool useThreadPool,
         Protocol protocolVersion, double ioTimeout);
    void run() override;

    Protocol protocolVersion() const override { return mProtocolVersion; }

    // INetworkConnections interface
    Future<Reference<IConnection>> connect(NetworkAddress toAddr) override;
    Reference<IListener> listen(NetworkAddress localAddr) override;

    // INetwork interface
    double now() override { return currentTime; };
    Future<Void> delay(double seconds, TaskId taskID) override;
    Future<class Void> yield(TaskId taskID) override;
    bool check_yield(TaskId taskID) override;
    TaskId getCurrentTask() override { return currentTaskID; };
    void setCurrentTask(TaskId taskID) override
    {
        taskStatCollection.updateCurrentTask(taskID, now());
        currentTaskID = taskID;
    };
    void onMainThread(Promise<Void>&& signal, TaskId taskID) override;
    IFailureMonitor& failureMonitor() override { return failMon; }
    FlowTransport& transport() override { return flowTransport; }
    INetworkConnections*& net() override { return network; }
    // Network currently has 2 possible modes: Normal and Recovery
    // The task priorities are dependent on the network mode
    NetworkMode getMode() override { return ready.readyQueue.getMode(); }
    void setMode(NetworkMode newMode) override
    {
        ready.readyQueue.setMode(newMode);
    };

    uint16_t getTaskPriority(TaskId taskID) override
    {
        // Return the raw priority of this task given the current system state
        if (getMode() == NetworkMode::NORMAL) {
            return taskID.getNormalPriority();
        } else if (getMode() == NetworkMode::RECOVERY) {
            return taskID.getRecoveryPriority();
        } else {
            ASSERT(0);
            return taskID.getNormalPriority();
        }
    }

    void stop() override
    {
        if (thread_network == this)
            stopImmediately();
        else
            // SOMEDAY: NULL for deferred error, no analysis of correctness
            // (itp)
            onMainThreadVoid([this] { this->stopImmediately(); }, NULL);
    }

    bool isSimulated() const override { return false; }
    Reference<IThreadPool> createThreadPool(bool forceThreads) override;
    THREAD_HANDLE startThread(THREAD_FUNC_RETURN (*func)(void*),
                              void* arg) override;
    void waitFor(Future<Void> what) override;
    Future<Reference<class IAsyncFile>> open(std::string filename,
                                             int64_t flags,
                                             int64_t mode) override;
    Future<Void> deleteFile(std::string filename, bool mustBeDurable) override;
    Future<Void> killFile(const std::string filename) override;
    Future<Void> clearFile(const std::string filename) override;
    Future<Void> deleteDir(std::string filename) override;
    void getDiskBytes(std::string const& directory, int64_t& free,
                      int64_t& total) override;
    bool isAddressOnThisHost(NetworkAddress const& addr) override;

    ACTOR static void callInFork(std::function<Future<Void>()> fun)
    {
        Void _uvar = wait(fun());
    }

    static void killImpl(int64_t pid, bool sigKill)
    {
#if defined(__unixish__)
        kill(pid, sigKill ? SIGKILL : SIGTERM);
#else
        throw internal_error();
#endif
    }

    // tries to terminate for 1 second - then will kill
    ACTOR static void killChild(int64_t pid)
    {
        state double beginTime = g_network->now();
        loop
        {
            killImpl(pid, g_network->now() - beginTime > 1.0);
            Void _uvar = wait(::delay(0.1));
        }
    }

    ACTOR static Future<int> waitPidImpl(int64_t pid)
    {
        loop
        {
            try {
                int status;
                auto res = waitpid(pid_t(pid), &status, WNOHANG);
                if (res == -1) {
                    throw platform_error();
                }
                if (res != 0) {
                    return WEXITSTATUS(status);
                }
                Void _uvar = wait(::delay(0.1));
            } catch (Error& e) {
                if (e.code() == error_code_actor_cancelled) {
                    killChild(pid);
                }
                throw;
            }
        }
    }

    Future<int> fork(std::function<Future<Void>()> fun) override
    {
#if defined(__unixish__)
        auto pid = ::fork();
        if (pid < 0) {
            throw platform_error();
        }
        if (pid == 0) {
            callInFork(fun);
            return Never();
        }
        return waitPidImpl(pid);
#else
        throw platform_error();
#endif
    }

    bool isIoTimedOut() override;
    void setIoTimedOut(bool value) override;
    void updateNow() { currentTime = timer_monotonic(); }

    bool useThreadPool;
    // private:

#ifdef __linux__
    std::unique_ptr<class IOUringContext> ioUringContext;
#endif
    Reference<IReactor> reactor;
    INetworkConnections* network; // initially this, but can be changed

    int64_t tsc_begin, tsc_end;
    TaskId currentTaskID;
    int64_t tasksIssued = 0;
    SimpleFailureMonitor failMon;
    FlowTransport flowTransport;
    double currentTime;
    bool stopped;
    bool ioTimedOut;
    std::map<uint32_t, bool> addressOnHostCache;
    struct TaskStats
    {
        // scheduling stats: ready queue and runtime
        IntervalCounter<> readyCount;
        IntervalCounter<double> latencyCount;
        IntervalCounter<double> runtimeCount;
        std::string taskName = "UNINITIALIZED";
        double tsBeginLatencyMeasure = 0;
        void endInterval(double curTime)
        {
            readyCount.endInterval(curTime);
            latencyCount.endInterval(curTime);
            runtimeCount.endInterval(curTime);
            latencyCount.resetCount();
            runtimeCount.resetCount();
        }
        bool lastIntervalEmpty()
        {
            return readyCount.getLastIntervalAverage() == 0 &&
                   latencyCount.getLastCountAverage() == 0 &&
                   runtimeCount.getLastIntervalAverage() == 0;
        }
    };
    typedef std::map<TaskIndex, TaskStats> TaskStatMap;

    // Task Metrics
    // We update counters every time a task is enqueued, dequeued, and
    // when the current running task is updated.  Periodically we crunch
    // these numbers to calculate individual task latency,
    // enqueue/dequeue rates, queue sizes, and total runtimes Latency
    // here is the average duration between times instances of this
    // taskID run from the ready queue, as long as instances of this
    // taskID exist on the ready queue
    class TaskStatCollection
    {
    private:
        TaskStatMap topKTaskStatMap;
        TaskStatMap allTaskStatMap;
        IntervalCounter<uint64_t> readyTaskTotalCount;
        IntervalCounter<uint64_t> delayTaskTotalCount;
        double topKNextTaskReportTime;
        double topKLastTaskReportTime;
        double allNextTaskReportTime;
        double allLastTaskReportTime;
        double tsLastTaskChange = 0;
        UID logUID;

    public:
        TaskStatCollection() {}
        void init(double tsNow, UID netLogUID)
        {
            topKNextTaskReportTime =
                tsNow + FLOW_KNOBS->TRACE_TASK_GRANULAR_INTERVAL;
            topKLastTaskReportTime = tsNow;
            allNextTaskReportTime = tsNow + FLOW_KNOBS->TRACE_TASK_ALL_INTERVAL;
            allLastTaskReportTime = tsNow;
            logUID = netLogUID;
// initialize taskStats for all known tasks
// saving task name in taskStat object facilitates ordering
#define TASK_ID(Task, index, recoveryPri, normalPri)                           \
    topKTaskStatMap[TaskIndex::Task].endInterval(tsNow);                       \
    topKTaskStatMap[TaskIndex::Task].taskName = #Task "Stats";                 \
    allTaskStatMap[TaskIndex::Task].endInterval(tsNow);                        \
    allTaskStatMap[TaskIndex::Task].taskName = #Task "Stats";
#include "tasks.inc"
#undef TASK_ID
        }

        void pushDelayTask() { delayTaskTotalCount += 1; }
        void popDelayTask() { delayTaskTotalCount -= 1; }
        void pushReadyTask(TaskId taskID, double tsNow)
        {
            pushReadyTaskCommon(topKTaskStatMap[taskID.getTaskIndex()], tsNow);
            pushReadyTaskCommon(allTaskStatMap[taskID.getTaskIndex()], tsNow);
            readyTaskTotalCount += 1;
        }
        void pushReadyTaskCommon(TaskStats& taskStats, double tsNow)
        {
            // Its necessary to allow for dynamic allocation because I think its
            // still possible for the code to come up with dynamic taskID
            // values. Now that the priority and ID are separate fields in
            // taskID, in future we should ensure this never happens
            if (taskStats.readyCount.getCount() == 0) {
                taskStats.tsBeginLatencyMeasure = tsNow;
            }
            taskStats.readyCount += 1;
        }
        void popReadyTask(TaskId taskID, double tsNow)
        {
            popReadyTaskCommon(topKTaskStatMap[taskID.getTaskIndex()], tsNow);
            popReadyTaskCommon(allTaskStatMap[taskID.getTaskIndex()], tsNow);
            readyTaskTotalCount -= 1;
        }
        void popReadyTaskCommon(TaskStats& taskStats, double tsNow)
        {
            assert(taskStats.tsBeginLatencyMeasure != 0);
            taskStats.readyCount -= 1;
            taskStats.latencyCount += tsNow - taskStats.tsBeginLatencyMeasure;
            taskStats.tsBeginLatencyMeasure =
                (taskStats.readyCount.getCount() > 0) ? tsNow : 0;
        };
        void updateCurrentTask(TaskId taskID, double tsNow)
        {
            updateCurrentTaskCommon(topKTaskStatMap[taskID.getTaskIndex()],
                                    tsNow);
            updateCurrentTaskCommon(allTaskStatMap[taskID.getTaskIndex()],
                                    tsNow);
            tsLastTaskChange = tsNow;
        }
        void updateCurrentTaskCommon(TaskStats& taskStats, double tsNow)
        {
            taskStats.runtimeCount += (tsNow - tsLastTaskChange);
        }
        void updateTaskStatsForReport(TaskStats& taskStats, double tsNow)
        {
            if (taskStats.readyCount.getCount() > 0) {
                // include latency for tasks waiting over multiple
                // measurement time slices
                taskStats.latencyCount +=
                    tsNow - taskStats.tsBeginLatencyMeasure;
            }
            taskStats.endInterval(tsNow);
        }
        void tryTraceTasks(double tsNow)
        {
            tryTraceTasksCommon(topKTaskStatMap, tsNow,
                                FLOW_KNOBS->TRACE_TASK_GRANULAR_INTERVAL,
                                topKNextTaskReportTime, topKLastTaskReportTime,
                                FLOW_KNOBS->TRACE_TASK_TOP_K);
            tryTraceTasksCommon(allTaskStatMap, tsNow,
                                FLOW_KNOBS->TRACE_TASK_ALL_INTERVAL,
                                allNextTaskReportTime, allLastTaskReportTime,
                                allTaskStatMap.size());
        }
        void tryTraceTasksCommon(TaskStatMap& taskStatMap, double tsNow,
                                 double taskStatReportInterval,
                                 double& nextTaskReportTime,
                                 double& lastTaskReportTime, int numToReport)
        {
            if (tsNow < nextTaskReportTime) {
                return;
            }
            double reportTimeSlice = tsNow - lastTaskReportTime;
            if (reportTimeSlice <= 0) {
                // belt and braces check
                return;
            }
            bool reportAll = numToReport >= taskStatMap.size();

// update task count averages
// will also set task name if not already set
#define TASK_ID(Task, index, recoveryPri, normalPri)                           \
    updateTaskStatsForReport(taskStatMap[TaskIndex::Task], tsNow);
#include "tasks.inc"
#undef TASK_ID

            // calculate latency, runtime, and push/pop rates for each
            // taskID then report each task's stats individually
            std::set<TaskStats*, bool (*)(TaskStats*, TaskStats*)>
            tasksOrderQSize([](TaskStats* a, TaskStats* b) -> bool {
                return (a->readyCount.getLastIntervalAverage() !=
                        b->readyCount.getLastIntervalAverage())
                           ? a->readyCount.getLastIntervalAverage() >
                                 b->readyCount.getLastIntervalAverage()
                           : a->taskName > b->taskName; // necessary to
                                                        // differentiate stats
                                                        // with the same count
                                                        // value
            });

            std::set<TaskStats*, bool (*)(TaskStats*, TaskStats*)>
            tasksOrderLatency([](TaskStats* a, TaskStats* b) -> bool {
                return (a->latencyCount.getLastCountAverage() !=
                        b->latencyCount.getLastCountAverage())
                           ? a->latencyCount.getLastCountAverage() >
                                 b->latencyCount.getLastCountAverage()
                           : a->taskName > b->taskName; // necessary to
                                                        // differentiate stats
                                                        // with the same count
                                                        // value
            });
            std::set<TaskStats*, bool (*)(TaskStats*, TaskStats*)>
            tasksOrderRuntime([](TaskStats* a, TaskStats* b) -> bool {
                return (a->runtimeCount.getLastIntervalAverage() !=
                        b->runtimeCount.getLastIntervalAverage())
                           ? a->runtimeCount.getLastIntervalAverage() >
                                 b->runtimeCount.getLastIntervalAverage()
                           : a->taskName > b->taskName; // necessary to
                                                        // differentiate stats
                                                        // with the same count
                                                        // value
            });
            std::set<TaskStats*, bool (*)(TaskStats*, TaskStats*)>
            tasksReported([](TaskStats* a, TaskStats* b) -> bool {
                return (a->runtimeCount.getLastIntervalAverage() !=
                        b->runtimeCount.getLastIntervalAverage())
                           ? a->runtimeCount.getLastIntervalAverage() >
                                 b->runtimeCount.getLastIntervalAverage()
                           : a->taskName > b->taskName; // necessary to
                                                        // differentiate stats
                                                        // with the same count
                                                        // value
            });

            if (reportAll) {
// order all tasks based on runtime
#define TASK_ID(Task, index, recoveryPri, normalPri)                           \
    tasksReported.insert(&taskStatMap[TaskIndex::Task]);
#include "tasks.inc"
#undef TASK_ID
            } else {
// get top K queue counts
// get top K latency
// get top K runtimes
// order all tasks based on runtime
#define TASK_ID(Task, index, recoveryPri, normalPri)                           \
    tasksOrderQSize.insert(&taskStatMap[TaskIndex::Task]);                     \
    tasksOrderLatency.insert(&taskStatMap[TaskIndex::Task]);                   \
    tasksOrderRuntime.insert(&taskStatMap[TaskIndex::Task]);
#include "tasks.inc"
#undef TASK_ID
                auto tasksOrderQSizeIt = tasksOrderQSize.begin();
                auto tasksOrderLatencyIt = tasksOrderLatency.begin();
                auto tasksOrderRuntimeIt = tasksOrderRuntime.begin();
                for (int i = 0; i < FLOW_KNOBS->TRACE_TASK_TOP_K; i++) {
                    if (tasksOrderQSizeIt != tasksOrderQSize.end()) {
                        tasksReported.insert(*tasksOrderQSizeIt);
                        tasksOrderQSizeIt++;
                    }
                    if (tasksOrderLatencyIt != tasksOrderLatency.end()) {
                        tasksReported.insert(*tasksOrderLatencyIt);
                        tasksOrderLatencyIt++;
                    }
                    if (tasksOrderRuntimeIt != tasksOrderRuntime.end()) {
                        tasksReported.insert(*tasksOrderRuntimeIt);
                        tasksOrderRuntimeIt++;
                    }
                }
            }

            // report the stats
            // the encoding is as following:
            // (qCount avg/s) (qCount max), qLatency avg max, runtime avg/s
            std::string taskTraceName =
                reportAll ? "TaskStatsAll" : "TaskStatsTop";
            TraceEvent te(taskTraceName.c_str());
            te.detail("ReadyCount", readyTaskTotalCount.getLastMax());
            te.detail("DelayCount", delayTaskTotalCount.getLastMax());
            readyTaskTotalCount.endInterval(tsNow);
            delayTaskTotalCount.endInterval(tsNow);
            for (auto tsIt : tasksReported) {
                if (!tsIt->lastIntervalEmpty()) {
                    te.detailpf(tsIt->taskName.c_str(), "%g %llu, %g %g, %g",
                                tsIt->readyCount.getLastIntervalAverage(),
                                tsIt->readyCount.getLastMax(),
                                tsIt->latencyCount.getLastCountAverage(),
                                tsIt->latencyCount.getLastMax(),
                                tsIt->runtimeCount.getLastIntervalAverage());
                }
            }
            nextTaskReportTime = tsNow + taskStatReportInterval;
            lastTaskReportTime = tsNow;
        }
    };
    TaskStatCollection taskStatCollection;

    uint64_t numYields;

    double lastPriorityTrackTime;
    TaskId lastMinTaskID;
    double priorityTimer[NetworkMetrics::PRIORITY_BINS];

    struct RecoveryTask;

    struct OrderedTask
    {
        TaskId taskID;
        Task* task;

        OrderedTask(TaskId taskID, Task* task)
          : taskID(taskID)
          , task(task)
        {}
    };
    struct OrderedTaskInReadyQueue
    {
        TaskId taskID;
        Task* task;
        int64_t normalPriority;
        int64_t recoveryPriority;
        OrderedTaskInReadyQueue(TaskId taskID, Task* task,
                                int64_t normalPriority,
                                int64_t recoveryPriority)
          : taskID(taskID)
          , task(task)
          , normalPriority(normalPriority)
          , recoveryPriority(recoveryPriority)
        {}
        ~OrderedTaskInReadyQueue() {}
    };
    // Uses enqueue timestamp in its comparisons. Records the taskNumber (ie
    // value of network.tasksIssued) when created.
    struct DelayedTask
    {
        double at;
        int64_t taskNumber;
        TaskId taskID;
        Task* task;
        DelayedTask(double at, int64_t taskNumber, TaskId taskID, Task* task)
          : taskNumber(taskNumber)
          , at(at)
          , taskID(taskID)
          , task(task)
        {}
        bool operator<(DelayedTask const& rhs) const
        {
            return at > rhs.at;
        } // Ordering is reversed for priority_queue
    };

    UID logUID;
    class OrderedTaskQueue
    {
        vector<OrderedTaskInReadyQueue> vec;
        static bool normalCompare(OrderedTaskInReadyQueue const& fst,
                                  OrderedTaskInReadyQueue const& snd)
        {
            return (fst.normalPriority < snd.normalPriority);
        }
        static bool recoveryCompare(OrderedTaskInReadyQueue const& fst,
                                    OrderedTaskInReadyQueue const& snd)
        {
            return (fst.recoveryPriority < snd.recoveryPriority);
        }
        NetworkMode mode;

    public:
        OrderedTaskQueue(NetworkMode mode = NetworkMode::NORMAL)
          : mode(mode)
        {
            vec.reserve(8192);
        }
        void pop()
        {
            pop_heap(vec.begin(), vec.end(),
                     (mode == NetworkMode::NORMAL) ? &normalCompare
                                                   : &recoveryCompare);
            vec.pop_back();
        }
        OrderedTaskInReadyQueue* top() { return vec.data(); }
        void push(OrderedTaskInReadyQueue orderedTask)
        {
            vec.push_back(orderedTask);
            push_heap(vec.begin(), vec.end(),
                      (mode == NetworkMode::NORMAL) ? &normalCompare
                                                    : &recoveryCompare);
        }
        void swap(OrderedTaskQueue& rhs)
        {
            vec.swap(rhs.vec);
            NetworkMode temp = mode;
            mode = rhs.mode;
            rhs.mode = temp;
        }
        bool empty() const { return vec.empty(); }
        void setMode(NetworkMode newMode)
        {
            if (mode == newMode)
                return;
            mode = newMode;
            if (mode == NetworkMode::NORMAL)
                make_heap(vec.begin(), vec.end(), &normalCompare);
            else if (mode == NetworkMode::RECOVERY) {
                make_heap(vec.begin(), vec.end(), &recoveryCompare);
            } else {
                ASSERT(0);
                make_heap(vec.begin(), vec.end(), &normalCompare);
            }
        }
        NetworkMode getMode() { return mode; }
    };

    ThreadSafeQueue<OrderedTask> threadReady;
    std::priority_queue<DelayedTask, std::vector<DelayedTask>> timers;

    // This queue class actually handles 2 separate priority queues.
    // Whatever the Network running mode, all tasks are always queued
    // and accesible according to the correct priority

    // This is a template so we can inject a fake Net2-like object for tests,
    // while still getting static dispatch in production.
    struct ModeBasedTaskQueue
    {
        Net2& network;
        OrderedTaskQueue readyQueue;
        explicit ModeBasedTaskQueue(Net2& n)
          : network(n){};
        bool empty() const { return readyQueue.empty(); }

        // explicit accessor method so we don't have to create an
        // OrderedTask object whenever someone wants to get a member
        // from the top task
        const OrderedTaskInReadyQueue* topOrderedTask()
        {
            return readyQueue.top();
        }

        // The justification for overloading calculatePriority and dispatching
        // based on the type deduced for T in the |push| template below is that
        // I didn't want to duplicate the other logic in |push|. I considered
        // making calculatePriority a member of both OrderedTask and
        // DelayedTask, but OrderedTask does not have access to the Net2
        // instance.
        void calculatePriority(const OrderedTask& value, int64_t* normal,
                               int64_t* recovery)
        {
            auto taskNumber = ++network.tasksIssued;
            *normal = (int64_t{ value.taskID.getNormalPriority() } << 32) -
                      taskNumber;
            *recovery = (int64_t{ value.taskID.getRecoveryPriority() } << 32) -
                        taskNumber;
        }
        void calculatePriority(const DelayedTask& value, int64_t* normal,
                               int64_t* recovery)
        {
            *normal = (int64_t{ value.taskID.getNormalPriority() } << 32) -
                      value.taskNumber;
            *recovery = (int64_t{ value.taskID.getRecoveryPriority() } << 32) -
                        value.taskNumber;
        }
        // pushes task objects to both priority queues
        // updates task stats
        template <class T>
        void push(const T& value)
        {
            network.taskStatCollection.pushReadyTask(value.taskID,
                                                     network.now());
            int64_t normal = 0;
            int64_t recovery = 0;
            calculatePriority(value, &normal, &recovery);
            readyQueue.push(OrderedTaskInReadyQueue(value.taskID, value.task,
                                                    normal, recovery));
        }
        // pops next task from priority queue used in this network mode,
        // and deletes counterpart task from the other priority queue
        // updates task stats
        void pop()
        {
            // update task stats
            double tsNow = network.now();
            network.taskStatCollection.popReadyTask(topOrderedTask()->taskID,
                                                    tsNow);
            network.taskStatCollection.updateCurrentTask(
                topOrderedTask()->taskID, tsNow);
            readyQueue.pop();
        }

        NetworkMode getMode() { return readyQueue.getMode(); }
        void setMode(NetworkMode newMode) { readyQueue.setMode(newMode); }
    };

    ModeBasedTaskQueue ready;
    const Protocol mProtocolVersion;

    PacketID sendPacket(ISerializeSource const& what,
                        const Endpoint& destination, bool reliable);
    bool check_yield(TaskId taskID, bool isRunLoop);
    void processThreadReady();
    void trackMinPriority(TaskId minTaskID, double tsNow);
    void stopImmediately()
    {
        stopped = true;
        decltype(ready.readyQueue) _1;
        ready.readyQueue.swap(_1);
        decltype(timers) _2;
        timers.swap(_2);
    }
}; // namespace N2

// TODO(anoyes) We should come up with a way to make tests in an actor file not
// generate actors unless necessary.
TEST_CASE("net2/priority order")
{
    ASSERT(g_net2 != nullptr);
    Net2::ModeBasedTaskQueue ready{ *g_net2 };
    std::vector<Net2::OrderedTask> tasks = {
        { TaskMinPriority, (Task*)0x1 },
        { TaskMinPriority, (Task*)0x2 },
        { TaskMaxPriority, (Task*)0x3 },
        { TaskMaxPriority, (Task*)0x4 },
    };
    for (const auto& task : tasks) {
        ready.push(task);
    }
    ASSERT(ready.topOrderedTask()->task == (Task*)0x3);
    ready.pop();
    ASSERT(ready.topOrderedTask()->task == (Task*)0x4);
    ready.pop();
    ASSERT(ready.topOrderedTask()->task == (Task*)0x1);
    ready.pop();
    ASSERT(ready.topOrderedTask()->task == (Task*)0x2);

    return Void();
}

TEST_CASE("net2/set current task id")
{
    ASSERT(g_net2 != nullptr);
    g_net2->setCurrentTask(TaskDefaultDelay);
    ASSERT(g_net2->getCurrentTask().getTaskIndex() ==
           TaskDefaultDelay.getTaskIndex());
    g_net2->setCurrentTask(TaskDefaultYield);
    ASSERT(g_net2->getCurrentTask().getTaskIndex() ==
           TaskDefaultYield.getTaskIndex());

    return Void();
}

TEST_CASE("net2/current task id after pop from ready queue")
{
    ASSERT(g_net2 != nullptr);
    g_net2->setCurrentTask(TaskDefaultDelay);
    ASSERT(g_net2->getCurrentTask().getTaskIndex() ==
           TaskDefaultDelay.getTaskIndex());
    Void _uvar = wait(g_net2->delay(0, TaskMaxPriority));
    ASSERT(g_net2->getCurrentTask().getTaskIndex() ==
           TaskMaxPriority.getTaskIndex());

    return Void();
}

TEST_CASE("net2/reorderQueue")
{
    ASSERT(g_net2 != nullptr);
    Net2::ModeBasedTaskQueue ready{ *g_net2 };
    std::vector<Net2::OrderedTask> tasks = { { TaskMinPriority, (Task*)0x1 },
                                             { TaskMinPriority, (Task*)0x2 },
                                             { TaskDiskWrite, (Task*)0x3 },
                                             { TaskDataDistribution,
                                               (Task*)0x4 },
                                             { TaskMaxPriority, (Task*)0x5 } };
    for (const auto& task : tasks) {
        ready.push(task);
    }
    ASSERT(ready.topOrderedTask()->task == (Task*)0x5);
    ready.pop();
    ASSERT(ready.topOrderedTask()->task == (Task*)0x4);
    ready.pop();
    ASSERT(ready.topOrderedTask()->task == (Task*)0x3);
    ready.pop();
    ASSERT(ready.topOrderedTask()->task == (Task*)0x1);
    ready.pop();
    ASSERT(ready.topOrderedTask()->task == (Task*)0x2);
    ready.pop();
    ASSERT(ready.empty());
    for (const auto& task : tasks) {
        ready.push(task);
    }
    ready.setMode(NetworkMode::RECOVERY);
    ASSERT(ready.topOrderedTask()->task == (Task*)0x5);
    ready.pop();
    ASSERT(ready.topOrderedTask()->task == (Task*)0x3);
    ready.pop();
    ASSERT(ready.topOrderedTask()->task == (Task*)0x4);
    ready.pop();
    ASSERT(ready.topOrderedTask()->task == (Task*)0x1);
    ready.pop();
    ASSERT(ready.topOrderedTask()->task == (Task*)0x2);
    ready.pop();
    ASSERT(ready.empty());
    ready.network.setMode(NetworkMode::NORMAL);
    return Void();
}

/*struct IThreadlike {
public:
        virtual void start() = 0;     // Call at most once!  Causes
run() to be called on the 'thread'. virtual ~IThreadlike() {}     //
Pre: start hasn't been called, or run() has returned virtual void
unblock() = 0;   // Pre: block() has been called by run(). Causes
block() to return.

protected:
        virtual void block() = 0;     // Call only from run().  Returns
when unblock() is called elsewhere. virtual void run() = 0;       // To
be overridden by client. Returning causes the thread to block until it
is destroyed.
};*/

struct Coroutine /*: IThreadlike*/
{
    Coroutine()
    {
        coro = Coro_new();
        if (coro == NULL)
            outOfMemory();
    }

    ~Coroutine() { Coro_free(coro); }

    void start()
    {
        int result = Coro_startCoro_(swapCoro(coro), coro, this, &entry);
        if (result == ENOMEM)
            outOfMemory();
    }

    void unblock()
    {
        // Coro_switchTo_( swapCoro(coro), coro );
        blocked.send(Void());
    }

protected:
    void block()
    {
        // Coro_switchTo_( swapCoro(main_coro), main_coro );
        blocked = Promise<Void>();
        double before = now();
        g_network->waitFor(blocked.getFuture());
        if (g_network->isSimulated() &&
            g_simulator.getCurrentProcess()->rebooting)
            TraceEvent("CoroUnblocked").detail("After", now() - before);
    }

    virtual void run() = 0;

private:
    void wrapRun()
    {
        run();
        Coro_switchTo_(swapCoro(main_coro), main_coro);
        // block();
    }

    static void entry(void* _this) { ((Coroutine*)_this)->wrapRun(); }

    Coro* coro;
    Promise<Void> blocked;
};

template <class Threadlike, class Mutex, bool IS_CORO>
class WorkPool : public IThreadPool,
                 public ReferenceCounted<WorkPool<Threadlike, Mutex, IS_CORO>>
{
    struct Worker;

    // Pool can survive the destruction of WorkPool while it waits for
    // workers to terminate
    struct Pool : ReferenceCounted<Pool>
    {
        Mutex queueLock;
        Deque<ThreadTask> work;
        vector<Worker*> idle, workers;
        ActorCollection anyError, allStopped;
        Future<Void> m_holdRefUntilStopped;

        Pool()
          : anyError(false)
          , allStopped(true)
        {
            m_holdRefUntilStopped = holdRefUntilStopped(this);
        }

        ~Pool()
        {
            for (int c = 0; c < workers.size(); c++)
                delete workers[c];
        }

        ACTOR Future<Void> holdRefUntilStopped(Pool* p)
        {
            p->addref();
            Void _uvar = wait(p->allStopped.getResult());
            p->delref();
            return Void();
        }
    };

    struct Worker : Threadlike
    {
        Pool* pool;
        IThreadPoolReceiver* userData;
        bool stop;
        ThreadReturnPromise<Void> stopped;
        ThreadReturnPromise<Void> error;

        Worker(Pool* pool, IThreadPoolReceiver* userData)
          : pool(pool)
          , userData(userData)
          , stop(false)
        {}

        void run() override
        {
            try {
                if (!stop)
                    userData->init();

                while (!stop) {
                    pool->queueLock.enter();
                    if (pool->work.empty()) {
                        pool->idle.push_back(this);
                        pool->queueLock.leave();
                        Threadlike::block();
                    } else {
                        auto front = pool->work.front();
                        pool->work.pop_front();
                        pool->queueLock.leave();
                        if (IS_CORO)
                            g_network->waitFor(delay(0, front.taskId));
                        (*front.action)(userData);
                    }
                }

                TraceEvent("CoroStop");
                delete userData;
                stopped.send(Void());
                return;
            } catch (Error& e) {
                TraceEvent("WorkPoolError").error(e, true);
                error.sendError(e);
            } catch (...) {
                TraceEvent("WorkPoolError");
                error.sendError(unknown_error());
            }

            try {
                delete userData;
            } catch (...) {
                TraceEvent(SevError, "WorkPoolErrorShutdownError");
            }
            stopped.send(Void());
        }
    };

    Reference<Pool> pool;
    Future<Void> m_stopOnError; // must be last, because its cancellation
                                // calls stop()!
    Error error;

    ACTOR Future<Void> stopOnError(WorkPool* w)
    {
        try {
            Void _uvar = wait(w->getError());
        } catch (Error& e) {
            w->error = e;
        }
        w->stop();
        return Void();
    }

    void checkError()
    {
        if (error.code() != invalid_error_code) {
            ASSERT(error.code() !=
                   error_code_successful_result); // Calling post or
                                                  // addThread after stop
                                                  // is an error
            throw error;
        }
    }

public:
    WorkPool()
      : pool(new Pool)
    {
        m_stopOnError = stopOnError(this);
    }

    Future<Void> getError() override { return pool->anyError.getResult(); }
    void addThread(IThreadPoolReceiver* userData) override
    {
        checkError();

        auto w = new Worker(pool.getPtr(), userData);
        pool->queueLock.enter();
        pool->workers.push_back(w);
        pool->queueLock.leave();
        pool->anyError.add(w->error.getFuture());
        pool->allStopped.add(w->stopped.getFuture());
        startWorker(w);
    }
    ACTOR static void startWorker(Worker* w)
    {
        // We want to make sure that coroutines are always started after
        // Net2::run() is called, so the main coroutine is
        // initialized.
        Void _uvar = wait(delay(0, g_network->getCurrentTask()));
        w->start();
    }
    void post(ThreadTask action) override
    {
        checkError();

        pool->queueLock.enter();
        pool->work.push_back(action);
        if (!pool->idle.empty()) {
            Worker* c = pool->idle.back();
            pool->idle.pop_back();
            pool->queueLock.leave();
            c->unblock();
        } else
            pool->queueLock.leave();
    }
    Future<Void> stop() override
    {
        if (error.code() == invalid_error_code)
            error = successful_result();

        pool->queueLock.enter();
        TraceEvent("WorkPool_Stop")
            .detail("Workers", pool->workers.size())
            .detail("Idle", pool->idle.size())
            .detail("Work", pool->work.size());

        for (int i = 0; i < pool->work.size(); i++)
            pool->work[i]
                .action->cancel(); // What if cancel() does something to this?
        pool->work.clear();
        for (int i = 0; i < pool->workers.size(); i++)
            pool->workers[i]->stop = true;

        vector<Worker*> idle;
        std::swap(idle, pool->idle);
        pool->queueLock.leave();

        for (int i = 0; i < idle.size(); i++)
            idle[i]->unblock();

        pool->allStopped.add(Void());

        return pool->allStopped.getResult();
    }
    bool isCoro() const override { return IS_CORO; }
    void addref() override { ReferenceCounted<WorkPool>::addref(); }
    void delref() override { ReferenceCounted<WorkPool>::delref(); }
};

typedef WorkPool<Coroutine, ThreadUnsafeSpinLock, true> CoroPool;

class ThreadPool : public IThreadPool, public ReferenceCounted<ThreadPool>
{
    struct Thread
    {
        ThreadPool* pool;
        IThreadPoolReceiver* userObject;
        Event stopped;
        static thread_local IThreadPoolReceiver* threadUserObject;
        explicit Thread(ThreadPool* pool, IThreadPoolReceiver* userObject)
          : pool(pool)
          , userObject(userObject)
        {}
        ~Thread() { ASSERT_ABORT(!userObject); }

        void run()
        {
            deprioritizeThread();

            threadUserObject = userObject;
            try {
                userObject->init();
                while (pool->ios.run_one() && !pool->mode)
                    ;
            } catch (Error& e) {
                TraceEvent(SevError, "ThreadPoolError").error(e);
            }
            delete userObject;
            userObject = 0;
            stopped.set();
        }
        static void dispatch(ThreadTask threadTask)
        {
            (*threadTask.action)(threadUserObject);
        }
    };
    THREAD_FUNC start(void* p)
    {
        ((Thread*)p)->run();
        THREAD_RETURN;
    }

    vector<Thread*> threads;
    boost::asio::io_service ios;
    boost::asio::io_service::work dontstop;
    enum Mode
    {
        Run = 0,
        Shutdown = 2
    };
    volatile int mode;

    struct ActionWrapper
    {
        ThreadTask task;
        explicit ActionWrapper(ThreadTask task)
          : task(task)
        {}
        // HACK: Boost won't use move constructors, so we just assume
        // the last copy made is the one that will be called or
        // cancelled
        ActionWrapper(ActionWrapper const& r)
          : task(r.task)
        {
            const_cast<ActionWrapper&>(r).task.action = nullptr;
        }
        void operator()()
        {
            Thread::dispatch(task);
            task.action = nullptr;
        }
        ~ActionWrapper()
        {
            if (task.action != nullptr) {
                task.action->cancel();
            }
        }

    private:
        void operator=(ActionWrapper const&);
    };

public:
    ThreadPool()
      : dontstop(ios)
      , mode(Run)
    {}
    ~ThreadPool() override {}
    Future<Void> stop() override
    {
        if (mode == Shutdown)
            return Void();
        ReferenceCounted<ThreadPool>::addref();
        ios.stop(); // doesn't work?
        mode = Shutdown;
        for (int i = 0; i < threads.size(); i++) {
            threads[i]->stopped.block();
            delete threads[i];
        }
        ReferenceCounted<ThreadPool>::delref();
        return Void();
    }
    Future<Void> getError() override { return Never(); } // FIXME
    void addref() override { ReferenceCounted<ThreadPool>::addref(); }
    void delref() override
    {
        if (ReferenceCounted<ThreadPool>::delref_no_destroy())
            stop();
    }
    void addThread(IThreadPoolReceiver* userData) override
    {
        threads.push_back(new Thread(this, userData));
        startThread(start, threads.back());
    }
    void post(ThreadTask action) override { ios.post(ActionWrapper(action)); }
};
thread_local IThreadPoolReceiver* ThreadPool::Thread::threadUserObject;

static tcp::endpoint
tcpEndpoint(NetworkAddress const& n)
{
    return tcp::endpoint(boost::asio::ip::address_v4(n.ip), n.port);
}

class BindPromise
{
    Promise<Void> p;
    const char* errContext;
    UID errID;

public:
    BindPromise(const char* errContext, UID errID)
      : errContext(errContext)
      , errID(errID)
    {}
    BindPromise(BindPromise const& r)
      : p(r.p)
      , errContext(r.errContext)
      , errID(r.errID)
    {}
    BindPromise(BindPromise&& r) noexcept(true)
      : p(std::move(r.p))
      , errContext(r.errContext)
      , errID(r.errID)
    {}

    Future<Void> getFuture() { return p.getFuture(); }

    void operator()(const boost::system::error_code& error,
                    size_t bytesWritten = 0)
    {
        try {
            if (error) {
                // Log the error...
                TraceEvent(SevWarn, errContext, errID)
                    .detail("Message", error.message());
                p.sendError(connection_failed());
            } else
                p.send(Void());
        } catch (Error& e) {
            p.sendError(e);
        } catch (...) {
            p.sendError(unknown_error());
        }
    }
};

class Connection : public IConnection, ReferenceCounted<Connection>
{
public:
    void addref() override { ReferenceCounted<Connection>::addref(); }
    void delref() override { ReferenceCounted<Connection>::delref(); }

    void close() override { closeSocket(); }

    explicit Connection(boost::asio::io_service& io_service)
      : id(g_nondeterministic_random->randomUniqueID())
      , socket(io_service)
    {}

    // This is not part of the IConnection interface, because it is
    // wrapped by INetwork::connect()
    ACTOR static Future<Reference<IConnection>> connect(
        boost::asio::io_service* ios, NetworkAddress addr)
    {
        state Reference<Connection> self(new Connection(*ios));

        self->peer_address = addr;
        try {
            auto to = tcpEndpoint(addr);
            BindPromise p("N2_ConnectError", self->id);
            Future<Void> onConnected = p.getFuture();
            self->socket.async_connect(to, std::move(p));

            Void _uvar = wait(onConnected);
            self->init();
            return self;
        } catch (Error&) {
            // Either the connection failed, or was cancelled by the
            // caller
            self->closeSocket();
            throw;
        }
    }

    // This is not part of the IConnection interface, because it is
    // wrapped by IListener::accept()
    void accept(NetworkAddress peerAddr)
    {
        this->peer_address = peerAddr;
        init();
    }

    // returns when write() can write at least one byte
    Future<Void> onWritable() override
    {
        ++g_network->networkMetrics.countWriteProbes;
        BindPromise p("N2_WriteProbeError", id);
        auto f = p.getFuture();
        socket.async_write_some(boost::asio::null_buffers(), std::move(p));
        return f;
    }

    // returns when read() can read at least one byte
    Future<Void> onReadable() override
    {
        ++g_network->networkMetrics.countReadProbes;
        BindPromise p("N2_ReadProbeError", id);
        auto f = p.getFuture();
        socket.async_read_some(boost::asio::null_buffers(), std::move(p));
        return f;
    }

    // Reads as many bytes as possible from the read buffer into
    // [begin,end) and returns the number of bytes read (might be 0)
    int read(uint8_t* begin, uint8_t* end) override
    {
        boost::system::error_code err;
        ++g_network->networkMetrics.countReads;
        size_t toRead = end - begin;
        size_t size = socket.read_some(
            boost::asio::mutable_buffers_1(begin, toRead), err);
        g_network->addBytesReceived(size);
        // TraceEvent("ConnRead", this->id).detail("Bytes", size);
        if (err) {
            if (err == boost::asio::error::would_block) {
                ++g_network->networkMetrics.countWouldBlock;
                return 0;
            }
            onReadError(err);
            throw connection_failed();
        }
        ASSERT(size); // If the socket is closed, we expect an 'eof'
                      // error, not a zero return value

        return size;
    }

    // Writes as many bytes as possible from the given SendBuffer chain
    // into the write buffer and returns the number of bytes written
    // (might be 0)
    int write(SendBuffer const* data) override
    {
        boost::system::error_code err;
        ++g_network->networkMetrics.countWrites;

        size_t toSend = 0;
        for (auto p = data; p; p = p->next)
            toSend += p->bytes_written - p->bytes_sent;
        ASSERT(toSend);
        size_t sent = socket.write_some(
            boost::iterator_range<SendBufferIterator>(SendBufferIterator(data),
                                                      SendBufferIterator()),
            err);

        if (err) {
            if (err == boost::asio::error::would_block) {
                ++g_network->networkMetrics.countWouldBlock;
                return 0;
            }
            onWriteError(err);
            throw connection_failed();
        }
        ASSERT(sent);

        return sent;
    }

    NetworkAddress getPeerAddress() override { return peer_address; }

    UID getDebugID() override { return id; }

    tcp::socket& getSocket() { return socket; }

private:
    UID id;
    tcp::socket socket;
    NetworkAddress peer_address;

    struct SendBufferIterator
    {
        typedef boost::asio::const_buffer value_type;
        typedef std::forward_iterator_tag iterator_category;
        typedef size_t difference_type;
        typedef boost::asio::const_buffer* pointer;
        typedef boost::asio::const_buffer& reference;

        SendBuffer const* p;

        SendBufferIterator(SendBuffer const* p = 0)
          : p(p)
        {}

        bool operator==(SendBufferIterator const& r) const { return p == r.p; }
        bool operator!=(SendBufferIterator const& r) const { return p != r.p; }
        void operator++() { p = p->next; }

        boost::asio::const_buffer operator*() const
        {
            return boost::asio::const_buffer(p->data + p->bytes_sent,
                                             p->bytes_written - p->bytes_sent);
        }
    };

    void init()
    {
        // Socket settings that have to be set after connect or accept
        // succeeds
        socket.non_blocking(true);
        socket.set_option(boost::asio::ip::tcp::no_delay(true));
    }

    void closeSocket()
    {
        boost::system::error_code error;
        socket.close(error);
        if (error)
            TraceEvent(SevWarn, "N2_CloseError", id)
                .detail("Message", error.value());
    }

    void onReadError(const boost::system::error_code& error)
    {
        TraceEvent(SevWarn, "N2_ReadError", id)
            .detail("Message", error.message());
        closeSocket();
    }
    void onWriteError(const boost::system::error_code& error)
    {
        TraceEvent(SevWarn, "N2_WriteError", id)
            .detail("Message", error.message());
        closeSocket();
    }
};

#ifdef __linux__
class IOUringSleep : public IOUringTask, public FastAllocated<IOUringSleep>
{
    double sleepTime;

public:
    IOUringSleep(double sleepTime)
      : sleepTime(sleepTime)
    {}

    void enqueue(io_uring_sqe* sqe) override
    {
        __kernel_timespec ts;
        auto secs = std::floor(sleepTime);
        ts.tv_sec = secs;
        ts.tv_nsec = 1e9 * (sleepTime - secs);
        io_uring_prep_timeout(sqe, &ts, 0, 0);
    }

    void setResult(int res) override { delete this; }
};

class IOUringContext : public ISubmitUring
{
    io_uring ring;
    bool didSubmit = false;

public:
    IOUringContext() { io_uring_queue_init(256, &ring, 0); }
    void enqueue(IOUringTask* task) override
    {
        auto sqe = io_uring_get_sqe(&ring);
        if (!sqe) {
            auto ret = io_uring_submit(&ring);
            if (ret < 0) {
                TraceEvent(SevError, "IOSubmitError").GetLastError();
                throw platform_error();
            }
            sqe = io_uring_get_sqe(&ring);
        }
        didSubmit = true;
        task->enqueue(sqe);
        io_uring_sqe_set_data(sqe, task);
    }

    unsigned poll()
    {
        unsigned res = 0;
        int err;
        io_uring_cqe* cqe;
        submit();
        while ((err = io_uring_peek_cqe(&ring, &cqe)) == 0) {
            auto t = reinterpret_cast<IOUringTask*>(io_uring_cqe_get_data(cqe));
            t->setResult(cqe->res);
            io_uring_cqe_seen(&ring, cqe);
            ++res;
        }
        ASSERT(err == 0 || -1*err == EAGAIN);
        return res > 0 ? res : 0;
    }

    void submit()
    {
        if (didSubmit) {
            io_uring_submit(&ring);
            didSubmit = true;
        }
    }

    void submitAndWait(double sleepTime, unsigned numTasks = 1)
    {
        if (sleepTime < 4e12) {
            enqueue(new IOUringSleep(sleepTime));
        }
        io_uring_submit_and_wait(&ring, 1);
        didSubmit = false;
    }
};

class IOUringReactor : public IReactor,
                       public IOUringTask,
                       public FastAllocated<IOUringReactor>,
                       ReferenceCounted<IOUringReactor>
{
    IOUringContext& context;
    Net2* network;
    int eventFD;
    std::atomic<bool> isSleeping;

public:
    IOUringReactor(IOUringContext& context, Net2* network)
      : context(context)
      , network(network)
      , isSleeping(false)
    {
        eventFD = ::eventfd(0, EFD_NONBLOCK);
    }

    void addref() override { ReferenceCounted<IOUringReactor>::addref(); }
    void delref() override { ReferenceCounted<IOUringReactor>::delref(); }

    void enqueue(io_uring_sqe* sqe) override
    {
        io_uring_prep_poll_add(sqe, eventFD, POLLIN);
    }

    void setResult(int r) override {
        int64_t buf;
        ::read(eventFD, &buf, sizeof(buf));
        // we just ignore the counter - this is only to wake us up
    }

    void sleepAndReact(double sleepTime) override
    {
        if (sleepTime > FLOW_KNOBS->BUSY_WAIT_THRESHOLD) {
            if (FLOW_KNOBS->REACTOR_FLAGS & 4) {
#ifdef __linux
                timespec tv;
                tv.tv_sec = 0;
                tv.tv_nsec = 20000;
                nanosleep(&tv, NULL);
#endif
            } else {
                sleepTime -= FLOW_KNOBS->BUSY_WAIT_THRESHOLD;
                isSleeping.store(true);
                context.submitAndWait(sleepTime);
                isSleeping.store(false);
            }
        } else if (sleepTime > 0) {
            if (!(FLOW_KNOBS->REACTOR_FLAGS & 8)) {
                threadYield();
            }
        }
        network->networkMetrics.countUringEvents += context.poll();
    }

    void wake() override
    {
        if (isSleeping.load()) {
            uint64_t val = 0;
            ::write(eventFD, &val, sizeof(val));
        }
    }
};

class IOUringConnection : public IConnection,
                          public FastAllocated<IOUringConnection>,
                          ReferenceCounted<IOUringConnection>
{
    IOUringContext& context;
    int fd;
    NetworkAddress address;
    UID id;
    bool inQueue = false;

    struct ConnectionTask : IOUringTask
    {
        int fd;
        short mask;
        Promise<Void> promise;
        explicit ConnectionTask(int fd, short mask)
          : fd(fd)
          , mask(mask)
        {}
        void enqueue(io_uring_sqe* sqe) override
        {
            io_uring_prep_poll_add(sqe, fd, mask);
        }
        void setResult(int res) override
        {
            //if (res & POLLERR) {
            //    int error = 0;
            //    socklen_t errlen = sizeof(error);
            //    getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &errlen);
            //    if (error != 0) {
            //        TraceEvent(SevWarnAlways, "SocketError")
            //            .detail("ErrorCode", error);
            //        promise.sendError(io_error());
            //        delete this;
            //        return;
            //    }
            //}
            //ASSERT(res | mask);
            promise.send(Void());
            delete this;
        }
    };

public:
    explicit IOUringConnection(IOUringContext& context, int fd,
                               NetworkAddress address)
      : context(context)
      , fd(fd)
      , address(address)
    {}

    static Future<Reference<IConnection>> connect(IOUringContext& context, NetworkAddress to) {
        auto fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        sockaddr_in addr;
        auto res = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        if (res < 0) {
            if (errno != EINPROGRESS) {
                throw io_error();
            }
        }
        return Reference<IConnection>(new IOUringConnection(context, fd, to));
    }

    void addref() override { ReferenceCounted<IOUringConnection>::addref(); }
    void delref() override { ReferenceCounted<IOUringConnection>::delref(); }
    void close() override { ::close(fd); }
    Future<Void> onWritable() override
    {
        auto t = new ConnectionTask(fd, POLLOUT);
        auto res = t->promise.getFuture();
        context.enqueue(t);
        return res;
    }
    Future<Void> onReadable() override
    {
        auto t = new ConnectionTask(fd, EPOLLIN);
        auto res = t->promise.getFuture();
        context.enqueue(t);
        return res;
    }
    int read(uint8_t* begin, uint8_t* end) override
    {
        auto res = ::read(fd, begin, end - begin);
        if (res < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return 0;
            }
            TraceEvent(SevError, "IOUringReadError").GetLastError();
            throw io_error();
        }
        return res;
    }
    int write(SendBuffer const* buffer) override
    {
        int sent = 0;
        auto current = buffer;
        while (current) {
            auto s = ::write(fd, current->data + current->bytes_sent,
                             current->bytes_written - current->bytes_sent);
            if (s < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return sent;
                }
                TraceEvent(SevError, "IOUringWriteError").GetLastError();
                throw io_error();
            } else if (s == 0) {
                return sent;
            }
            if (current->bytes_written - current->bytes_sent - sent == 0) {
                current = current->next;
            } else {
                return sent;
            }
        }
        return sent;
    }

    NetworkAddress getPeerAddress() override { return address; }
    UID getDebugID() override { return id; }
};

class IOUringListener : public IListener,
                        public FastAllocated<IOUringListener>,
                        ReferenceCounted<IOUringListener>
{
    IOUringContext& context;
    NetworkAddress listenAddress;
    int fd;

    static int createSocket()
    {
        auto res = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        int reuse = 1;
        if (setsockopt(res, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse))) {
            TraceEvent(SevError, "SetSockOptFailed").GetLastError();
            throw platform_error();
        }
        return res;
    }

    void bind()
    {
        sockaddr_in addr;
        ::memset(&addr, 0, sizeof(addr));
        addr.sin_port = htons(listenAddress.port);
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(listenAddress.ip);
        ::bind(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
        ::listen(fd, 64);
    }

    struct UringAcceptor : IOUringTask, FastAllocated<UringAcceptor>
    {
        IOUringContext& context;
        int fd;
        Promise<Reference<IConnection>> promise;
        sockaddr_in inAddr;
        socklen_t addrlen = sizeof(sockaddr_in);
        UringAcceptor(IOUringContext& context, int fd)
          : context(context)
          , fd(fd)
        {}
        void enqueue(io_uring_sqe* sqe)
        {
            io_uring_prep_poll_add(sqe, fd, POLLIN);
        }

        void setResult(int res)
        {
            if (res < 0) {
                promise.sendError(io_error());
            } else {
                auto resFD = ::accept(fd, reinterpret_cast<sockaddr*>(&inAddr), &addrlen);
                if (resFD <= 0) {
                    promise.sendError(io_error());
                }
                NetworkAddress remote;
                remote.port = ntohs(inAddr.sin_port);
                remote.ip = ntohl(inAddr.sin_addr.s_addr);
                promise.send(Reference<IConnection>(
                    new IOUringConnection(context, resFD, remote)));
            }
            delete this;
        }
    };

public:
    IOUringListener(IOUringContext& context, NetworkAddress listenAddress)
      : context(context)
      , listenAddress(listenAddress)
      , fd(createSocket())
    {
        bind();
    }
    void addref() override { ReferenceCounted<IOUringListener>::addref(); }
    void delref() override { ReferenceCounted<IOUringListener>::delref(); }

    Future<Reference<IConnection>> accept() override
    {
        auto a = new UringAcceptor(context, fd);
        context.enqueue(a);
        return a->promise.getFuture();
    }

    NetworkAddress getListenAddress() override { return listenAddress; }
};
#endif

class Listener : public IListener, ReferenceCounted<Listener>
{
    NetworkAddress listenAddress;
    tcp::acceptor acceptor;

public:
    Listener(boost::asio::io_service& io_service, NetworkAddress listenAddress)
      : listenAddress(listenAddress)
      , acceptor(io_service, tcpEndpoint(listenAddress))
    {}

    void addref() override { ReferenceCounted<Listener>::addref(); }
    void delref() override { ReferenceCounted<Listener>::delref(); }

    // Returns one incoming connection when it is available
    Future<Reference<IConnection>> accept() override { return doAccept(this); }

    NetworkAddress getListenAddress() override { return listenAddress; }

private:
    ACTOR static Future<Reference<IConnection>> doAccept(Listener* self)
    {
        state Reference<Connection> conn(
            new Connection(self->acceptor.get_io_service()));
        state tcp::acceptor::endpoint_type peer_endpoint;
        try {
            BindPromise p("N2_AcceptError", UID());
            auto f = p.getFuture();
            self->acceptor.async_accept(conn->getSocket(), peer_endpoint,
                                        std::move(p));
            Void _uvar = wait(f);
            conn->accept(
                NetworkAddress(peer_endpoint.address().to_v4().to_ulong(),
                               peer_endpoint.port()));

            return conn;
        } catch (...) {
            conn->close();
            throw;
        }
    }
};

struct PromiseTask : public Task, public FastAllocated<PromiseTask>
{
    Promise<Void> promise;
    PromiseTask() {}
    explicit PromiseTask(Promise<Void>&& promise) noexcept(true)
      : promise(std::move(promise))
    {}

    void operator()() override
    {
        promise.send(Void());
        delete this;
    }
};

Net2::Net2(NetworkAddress localAddress, bool useThreadPool,
           Protocol protocolVersion, double ioTimeout)
  : useThreadPool(useThreadPool)
#ifdef __linux__
  , ioUringContext(FLOW_KNOBS->ENABLE_IO_URING ? new IOUringContext : nullptr)
  , reactor(FLOW_KNOBS->ENABLE_IO_URING
                ? Reference<IReactor>(new IOUringReactor(*ioUringContext, this))
                : Reference<IReactor>(new ASIOReactor(this)))
#else
  , reactor(new ASIOReactor(this))
#endif
  , network(this)
  // Until run() is called, yield() will always yield
  , tsc_begin(0)
  , tsc_end(0)
  , currentTaskID(TaskDefaultYield)
  , tasksIssued(0)
  , stopped(false)
  , numYields(0)
  , lastMinTaskID(TaskMinPriority)
  , ready(*this)
  , logUID(g_random->randomUniqueID())
  , mProtocolVersion(protocolVersion)
  , ioTimedOut(false)
{
    TraceEvent("Net2Starting");

    int priBins[] = { 1, 2050, 3050, 4050, 4950, 5050, 7050, 8050, 10050 };
    static_assert(sizeof(priBins) ==
                      sizeof(int) * NetworkMetrics::PRIORITY_BINS,
                  "Fix priority bins");
    for (int i = 0; i < NetworkMetrics::PRIORITY_BINS; i++)
        networkMetrics.priorityBins[i] = priBins[i];

    Net2AsyncFile::init();
#if defined(__linux__) && !defined(WITH_UNDODB)
    if (FLOW_KNOBS->ENABLE_AIO && !FLOW_KNOBS->ENABLE_IO_URING) {
        AsyncFileKAIO::init(
            dynamic_cast<ASIOReactor*>(reactor.getPtr())->createEventFD(),
            ioTimeout);
    }
#endif
    updateNow();
}

void
Net2::run()
{
    TraceEvent("Net2Running");
    thread_network = this;

#ifdef WIN32
    if (timeBeginPeriod(1) != TIMERR_NOERROR)
        TraceEvent(SevError, "timeBeginPeriodError");
#endif

    startProfiling(this);

    if (!current_coro) {
        current_coro = main_coro = Coro_new();
        if (main_coro == NULL)
            outOfMemory();

        Coro_initializeMainCoro(main_coro);
        // printf("Main thread: %d bytes stack presumed available\n",
        // Coro_bytesLeftOnStack(current_coro));
    }

#if defined(__linux__) && SLOW_TASK_PROFILE
    net2backtraces = new void*[net2backtraces_max];
    void** other_backtraces = new void*[net2backtraces_max];

    sigset_t sigprof_set;
    sigemptyset(&sigprof_set);
    sigaddset(&sigprof_set, SIGPROF);
#endif

    double nnow = timer_monotonic();
    // init task stat collection
    taskStatCollection.init(nnow, logUID);

    while (!stopped) {
        ++networkMetrics.countRunLoop;

        taskStatCollection.tryTraceTasks(this->currentTime);
#if defined(__linux__) && !defined(WITH_UNDODB)
        if (!FLOW_KNOBS->ENABLE_IO_URING && FLOW_KNOBS->ENABLE_AIO) {
            AsyncFileKAIO::launch();
        }
#endif

#if defined(__linux__) && SLOW_TASK_PROFILE
        sigset_t orig_set;
        pthread_sigmask(SIG_BLOCK, &sigprof_set, &orig_set);

        size_t other_offset = net2backtraces_offset;
        bool was_overflow = net2backtraces_overflow;
        int signal_count = net2backtraces_count;

        if (other_offset) {
            void** _traces = net2backtraces;
            net2backtraces = other_backtraces;
            other_backtraces = _traces;

            net2backtraces_offset = 0;
        }

        net2backtraces_overflow = false;
        net2backtraces_count = 0;

        pthread_sigmask(SIG_SETMASK, &orig_set, NULL);

        if (signal_count) {
            TraceEvent("Net2SlowTaskSummary")
                .detail("SignalsReceived", signal_count)
                .detail("OverflowEncountered", was_overflow)
                .detail("BackTraceHarvested", other_offset != 0);
        }
        if (other_offset) {
            size_t iter_offset = 0;
            while (iter_offset < other_offset) {
                ProfilingSample* ps =
                    (ProfilingSample*)(other_backtraces + iter_offset);
                std::string buf;
                for (size_t frame_iter = 0; frame_iter < ps->length;
                     frame_iter++) {
                    buf += format("%p ", ps->frames[frame_iter]);
                }
                buf.resize(buf.size() - 1);
                TraceEvent(SevWarn, "Net2SlowTaskTrace")
                    .detailf("TraceTime", "{:.6f}", ps->timestamp)
                    .detail("Trace",
                            format("addr2line -e bin/fdbserver -p -C -f -i %s",
                                   buf.c_str()));
                iter_offset += ps->length + 2;
            }
        }

        // to keep the thread liveness check happy
        net2liveness = g_nondeterministic_random->randomInt(0, 100000);
#endif

        double sleepTime = 0;
        bool b = ready.empty();
        if (b) {
            b = threadReady.canSleep();
            if (!b)
                ++networkMetrics.countCantSleep;
        } else
            ++networkMetrics.countWontSleep;
        if (b) {
            sleepTime = 1e99;
            if (!timers.empty())
                sleepTime = timers.top().at - timer_monotonic(); // + 500e-6?
        }
        reactor->sleepAndReact(sleepTime);

        updateNow();
        double tsNow = this->currentTime;
        if (tsNow > nnow + FLOW_KNOBS->SLOW_LOOP_CUTOFF &&
            g_nondeterministic_random->random01() < 0.001)
            TraceEvent("SomewhatSlowRunloopTopx1000")
                .detail("Elapsed", tsNow - nnow);

        if (sleepTime)
            trackMinPriority(TaskMinPriority, tsNow);
        while (!timers.empty() && timers.top().at < tsNow) {
            ++networkMetrics.countTimers;
            taskStatCollection.popDelayTask();
            ready.push(timers.top());
            timers.pop();
        }

        processThreadReady();

        tsc_begin = __rdtsc();
        tsc_end = tsc_begin + FLOW_KNOBS->TSC_YIELD_TIME;
        numYields = 0;
        NetworkMode curMode = getMode();
        TaskId minTaskID = TaskMaxPriority;
        while (!ready.empty()) {
            ++networkMetrics.countTasks;
            if (curMode != getMode()) {
                // in case priority mode changes in middle of this ready
                // task batch
                curMode = getMode();
            }
            Task* task = ready.topOrderedTask()->task;
            currentTaskID = ready.topOrderedTask()->taskID;
            // The pop updates ready stats
            ready.pop();
            // for minimum task latency measurement.  Run after
            // currentTaskID is updated
            minTaskID =
                (getTaskPriority(minTaskID) > getTaskPriority(currentTaskID))
                    ? currentTaskID
                    : minTaskID;

            try {
                (*task)();
            } catch (Error& e) {
                TraceEvent(SevError, "TaskError").error(e);
            } catch (...) {
                TraceEvent(SevError, "TaskError").error(unknown_error());
            }

            if (check_yield(TaskMaxPriority, true)) {
                ++networkMetrics.countYields;
                break;
            }
        }

        auto newNnow = timer_monotonic();
        networkMetrics.secSquaredRunLoop +=
            (newNnow - nnow) * (newNnow - nnow) / 2.0;
        nnow = newNnow;

        if (nnow > tsNow + FLOW_KNOBS->SLOW_LOOP_CUTOFF &&
            g_nondeterministic_random->random01() < 0.001)
            TraceEvent("SomewhatSlowRunloopBottomx1000")
                .detail("Elapsed", nnow - tsNow);

        trackMinPriority(minTaskID, nnow);
    }

#ifdef WIN32
    timeEndPeriod(1);
#endif
}

void
Net2::trackMinPriority(TaskId minTaskID, double tsNow)
{
    // TODO(anoyes): Convince self this makes sense.
    if (minTaskID.getTaskIndex() != lastMinTaskID.getTaskIndex())
        for (int c = 0; c < NetworkMetrics::PRIORITY_BINS; c++) {
            int64_t pri = networkMetrics.priorityBins[c];
            if (pri >= getTaskPriority(minTaskID) &&
                pri < getTaskPriority(lastMinTaskID)) { // busy -> idle
                double busyFor = lastPriorityTrackTime - priorityTimer[c];
                networkMetrics.secSquaredPriorityBlocked[c] +=
                    busyFor * busyFor;
            }
            if (pri < getTaskPriority(minTaskID) &&
                pri >= getTaskPriority(lastMinTaskID)) { // idle -> busy
                priorityTimer[c] = tsNow;
            }
        }
    lastMinTaskID = minTaskID;
    lastPriorityTrackTime = tsNow;
}

void
Net2::processThreadReady()
{
    while (true) {
        Optional<OrderedTask> t = threadReady.pop();
        if (!t.present())
            break;
        ASSERT(t.get().task != 0);
        // push also updates the task metrics
        ready.push(t.get());
    }
}

bool
Net2::check_yield(TaskId taskID, bool isRunLoop)
{
    if (!isRunLoop && numYields > 0) {
        ++numYields;
        return true;
    }

    if (current_coro && Coro_bytesLeftOnStack(current_coro) < 65536) {
        ++networkMetrics.countYieldBigStack;
        return true;
    }

    processThreadReady();

    if (taskID.getTaskIndex() == TaskDefaultYield.getTaskIndex())
        taskID = currentTaskID;
    if (!ready.empty() &&
        comparePriority(taskID, ready.topOrderedTask()->taskID, getMode())) {
        return true;
    }

    int64_t tsc_now = __rdtsc();
    if (tsc_now < tsc_begin) {
        return true;
    }

    // Check for slow tasks, but only if we are calling from the run
    // loop
    if (isRunLoop) {
        double elapsed = tsc_now - tsc_begin;
        if (elapsed > FLOW_KNOBS->TSC_YIELD_TIME && tsc_begin > 0) {
            int i = std::min<double>(NetworkMetrics::SLOW_EVENT_BINS - 1,
                                     log(elapsed / 1e6) / log(2.));
            int s = ++networkMetrics.countSlowEvents[i];
            uint64_t warnThreshold = g_network->isSimulated() ? 10e9 : 500e6;
            // printf("SlowTask: %d, %d yields\n", (int)(elapsed/1e6),
            // numYields);
            if (!DEBUG_DETERMINISM &&
                (g_nondeterministic_random->random01() * 10e9 < elapsed ||
                 warnThreshold < elapsed))
                TraceEvent("SlowTask")
                    .detail("taskIndex", currentTaskID)
                    .detail("MClocks", elapsed / 1e6)
                    .detail("NumYields", numYields)
                    .detail("ThresholdExceeded", elapsed > warnThreshold);
        }
    }

    if (tsc_now > tsc_end) {
        ++numYields;
        return true;
    }

    tsc_begin = tsc_now;
    return false;
}

bool
Net2::check_yield(TaskId taskID)
{
    return check_yield(taskID, false);
}

Future<class Void>
Net2::yield(TaskId taskID)
{
    g_network->networkMetrics.countYieldCalls++;
    if (taskID.getTaskIndex() == TaskDefaultYield.getTaskIndex())
        taskID = currentTaskID;
    if (check_yield(taskID, false)) {
        g_network->networkMetrics.countYieldCallsTrue++;
        return delay(0, taskID);
    }
    g_network->setCurrentTask(taskID);
    return Void();
}

Future<Void>
Net2::delay(double seconds, TaskId taskID)
{
    if (seconds <= 0.) {
        PromiseTask* t = new PromiseTask;
        this->ready.push(OrderedTask(taskID, t));
        return t->promise.getFuture();
    }
    if (seconds >= 4e12) // Intervals that overflow an int64_t in microseconds
                         // (more than 100,000 years) are treated as infinite
        return Never();

    double at = now() + seconds;
    PromiseTask* t = new PromiseTask;
    taskStatCollection.pushDelayTask();
    this->timers.push(DelayedTask(at, ++tasksIssued, taskID, t));
    return t->promise.getFuture();
}

void
Net2::onMainThread(Promise<Void>&& signal, TaskId taskID)
{
    if (stopped)
        return;
    PromiseTask* p = new PromiseTask(std::move(signal));

    if (thread_network == this) {
        this->ready.push(OrderedTask(taskID, p));
    } else {
        if (threadReady.push(OrderedTask(taskID, p)))
            reactor->wake();
    }
}

Reference<IThreadPool>
Net2::createThreadPool(bool forceThreads)
{
    if (forceThreads || useThreadPool)
        return Reference<IThreadPool>(new ThreadPool);
    else
        return Reference<IThreadPool>(new CoroPool);
}

THREAD_HANDLE
Net2::startThread(THREAD_FUNC_RETURN (*func)(void*), void* arg)
{
    return ::startThread(func, arg);
}

ACTOR void
coroSwitcher(Future<Void> what, TaskId taskID, Coro* coro)
{
    try {
        state double t = now();
        Void _uvar = wait(what);
        // if (g_network->isSimulated() &&
        // g_simulator.getCurrentProcess()->rebooting && now()!=t)
        //	TraceEvent("NonzeroWaitDuringReboot").detail("TaskID",
        // taskID).detail("Elapsed", now()-t).backtrace("Flow");
    } catch (Error&) {
    }
    Void _uvar = wait(delay(0, taskID));
    Coro_switchTo_(swapCoro(coro), coro);
}

void
Net2::waitFor(Future<Void> what)
{
    ASSERT(current_coro != main_coro);
    if (what.isReady())
        return;
    Coro* c = current_coro;
    double t = now();
    coroSwitcher(what, g_network->getCurrentTask(), current_coro);
    Coro_switchTo_(swapCoro(main_coro), main_coro);
    // if (g_network->isSimulated() &&
    // g_simulator.getCurrentProcess()->rebooting && now()!=t)
    //	TraceEvent("NonzeroWaitDuringReboot").detail("TaskID",
    // currentTaskID).detail("Elapsed", now()-t).backtrace("Coro");
    ASSERT(what.isReady());
}

Future<Reference<IConnection>>
Net2::connect(NetworkAddress toAddr)
{
#ifdef __linux__
    if (FLOW_KNOBS->ENABLE_IO_URING) {
        return IOUringConnection::connect(*ioUringContext, toAddr);
    }
#endif
    return Connection::connect(
        &dynamic_cast<ASIOReactor*>(this->reactor.getPtr())->ios, toAddr);
}

bool
Net2::isAddressOnThisHost(NetworkAddress const& addr)
{
    auto it = addressOnHostCache.find(addr.ip);
    if (it != addressOnHostCache.end())
        return it->second;

    if (addressOnHostCache.size() > 50000)
        addressOnHostCache
            .clear(); // Bound cache memory; should not really happen

    try {
        boost::asio::io_service ioService;
        boost::asio::ip::udp::socket socket(ioService);
        boost::asio::ip::udp::endpoint endpoint(
            boost::asio::ip::address_v4(addr.ip), 1);
        socket.connect(endpoint);
        bool local =
            socket.local_endpoint().address().to_v4().to_ulong() == addr.ip;
        socket.close();
        if (local)
            TraceEvent(SevInfo, "AddressIsOnHost").detail("Address", addr);
        return addressOnHostCache[addr.ip] = local;
    } catch (boost::system::system_error e) {
        TraceEvent(SevWarnAlways, "IsAddressOnHostError")
            .detail("Address", addr)
            .detail("ErrDesc", e.what())
            .detail("ErrCode", e.code().value());
        return addressOnHostCache[addr.ip] = false;
    }
}

bool
Net2::isIoTimedOut()
{
    return ioTimedOut;
}

void
Net2::setIoTimedOut(bool value)
{
    ioTimedOut = value;
}

Reference<IListener>
Net2::listen(NetworkAddress localAddr)
{
    try {
#ifdef __linux__
        if (FLOW_KNOBS->ENABLE_IO_URING) {
            return Reference<IListener>(
                new IOUringListener(*ioUringContext, localAddr));
        }
#endif
        return Reference<IListener>(new Listener(
            dynamic_cast<ASIOReactor*>(reactor.getPtr())->ios, localAddr));
    } catch (boost::system::system_error const& e) {
        Error x;
        if (e.code().value() == EADDRINUSE)
            x = address_in_use();
        else if (e.code().value() == EADDRNOTAVAIL)
            x = invalid_local_address();
        else
            x = bind_failed();
        TraceEvent("Net2ListenError").detail("Message", e.what()).error(x);
        throw x;
    } catch (std::exception const& e) {
        Error x = unknown_error();
        TraceEvent("Net2ListenError").detail("Message", e.what()).error(x);
        throw x;
    } catch (...) {
        Error x = unknown_error();
        TraceEvent("Net2ListenError").error(x);
        throw x;
    }
}

Future<Reference<class IAsyncFile>>
Net2::open(std::string filename, int64_t flags, int64_t mode)
{
    if ((flags & IAsyncFile::OPEN_EXCLUSIVE)) {
        ASSERT(flags & IAsyncFile::OPEN_CREATE);
    }
    if (!(flags & IAsyncFile::OPEN_UNCACHED)) {
        return AsyncFileCached::open(filename, flags, mode);
    }
#if defined(__linux__) && !defined(WITH_UNDODB)
    if ((flags & IAsyncFile::OPEN_UNBUFFERED) &&
        !(flags & IAsyncFile::OPEN_NO_AIO) && FLOW_KNOBS->ENABLE_AIO) {
        if (FLOW_KNOBS->ENABLE_IO_URING) {
            // return AsyncFileIOUring::open(filename, flags, mode,
            // &reactor.ios);
        } else if (FLOW_KNOBS->ENABLE_AIO) {
            return AsyncFileKAIO::open(
                filename, flags, mode,
                &dynamic_cast<ASIOReactor*>(reactor.getPtr())->ios);
        }
    }
#endif
    return Net2AsyncFile::open(
        filename, flags, mode,
        &dynamic_cast<ASIOReactor*>(reactor.getPtr())->ios);
}

Future<Void>
Net2::deleteFile(std::string filename, bool mustBeDurable)
{
    return Net2AsyncFile::deleteFile(filename, mustBeDurable);
}

Future<Void>
Net2::killFile(const std::string filename)
{
    ASSERT(false);
    return Void();
}

Future<Void>
Net2::clearFile(const std::string filename)
{
    ASSERT(false);
    return Void();
}

Future<Void>
Net2::deleteDir(std::string filename)
{
    return Net2AsyncFile::deleteDirectory(filename);
}

void
Net2::getDiskBytes(std::string const& directory, int64_t& free, int64_t& total)
{
    return ::getDiskBytes(directory, free, total);
}

// namespace N2

#ifdef __linux__
#include <sys/prctl.h>
#include <pthread.h>
#include <sched.h>
#endif

ASIOReactor::ASIOReactor(Net2* net)
  : do_not_stop(ios)
  , network(net)
  , firstTimer(ios)
{
#ifdef __linux__
    // Reactor flags are used only for experimentation, and are
    // platform-specific
    if (FLOW_KNOBS->REACTOR_FLAGS & 1) {
        prctl(PR_SET_TIMERSLACK, 1, 0, 0, 0);
        printf("Set timerslack to 1ns\n");
    }

    if (FLOW_KNOBS->REACTOR_FLAGS & 2) {
        int ret;
        pthread_t this_thread = pthread_self();
        struct sched_param params;
        params.sched_priority = sched_get_priority_max(SCHED_FIFO);
        ret = pthread_setschedparam(this_thread, SCHED_FIFO, &params);
        if (ret != 0)
            printf("Error setting priority (%d %d)\n", ret, errno);
        else
            printf("Set scheduler mode to SCHED_FIFO\n");
    }
#endif
}

void
ASIOReactor::sleepAndReact(double sleepTime)
{
    if (sleepTime > FLOW_KNOBS->BUSY_WAIT_THRESHOLD) {
        if (FLOW_KNOBS->REACTOR_FLAGS & 4) {
#ifdef __linux
            timespec tv;
            tv.tv_sec = 0;
            tv.tv_nsec = 20000;
            nanosleep(&tv, NULL);
#endif
        } else {
            sleepTime -= FLOW_KNOBS->BUSY_WAIT_THRESHOLD;
            if (sleepTime < 4e12) {
                this->firstTimer.expires_from_now(
                    boost::posix_time::microseconds(int64_t(sleepTime * 1e6)));
                this->firstTimer.async_wait(&nullWaitHandler);
            }
            ios.run_one();
            this->firstTimer.cancel();
        }
        ++network->networkMetrics.countASIOEvents;
    } else if (sleepTime > 0) {
        if (!(FLOW_KNOBS->REACTOR_FLAGS & 8))
            threadYield();
    }
    while (ios.poll_one())
        ++network->networkMetrics.countASIOEvents; // Make this a task?
}

void
ASIOReactor::wake()
{
    ios.post(nullCompletionHandler);
}

} // namespace N2

INetwork*
newNet2(NetworkAddress localAddress, bool useThreadPool,
        Protocol protocolVersion, double fileIoTimeout)
{
    try {
        N2::g_net2 = new N2::Net2(localAddress, useThreadPool, protocolVersion,
                                  fileIoTimeout);
    } catch (boost::system::system_error e) {
        TraceEvent("Net2InitError").detail("Message", e.what());
        throw unknown_error();
    } catch (std::exception const& e) {
        TraceEvent("Net2InitError").detail("Message", e.what());
        throw unknown_error();
    }

    return N2::g_net2;
}

struct TestGVR
{
    Standalone<StringRef> key;
    int64_t version;
    Optional<std::pair<UID, UID>> debugID;
    Promise<Optional<Standalone<StringRef>>> reply;

    TestGVR() {}

    template <class Ar>
    void serialize(Ar& ar)
    {
        ar& key& version& debugID& reply;
    }
};

template <class F>
void
startThreadF(F&& func)
{
    struct Thing
    {
        F f;
        Thing(F&& f)
          : f(std::move(f))
        {}
        THREAD_FUNC start(void* p)
        {
            Thing* self = (Thing*)p;
            self->f();
            delete self;
            THREAD_RETURN;
        }
    };
    Thing* t = new Thing(std::move(func));
    startThread(Thing::start, t);
}

void net2_test(){
    /*printf("ThreadSafeQueue test\n");
    printf("  Interface: ");
    ThreadSafeQueue<int> tq;
    ASSERT( tq.canSleep() == true );

    ASSERT( tq.push( 1 ) == true ) ;
    ASSERT( tq.push( 2 ) == false );
    ASSERT( tq.push( 3 ) == false );

    ASSERT( tq.pop().get() == 1 );
    ASSERT( tq.pop().get() == 2 );
    ASSERT( tq.push( 4 ) == false );
    ASSERT( tq.pop().get() == 3 );
    ASSERT( tq.pop().get() == 4 );
    ASSERT( !tq.pop().present() );
    printf("OK\n");

    printf("Threaded: ");
    Event finished, finished2;
    int thread1Iterations = 1000000, thread2Iterations = 100000;

    if (thread1Iterations)
            startThreadF([&](){
                    printf("Thread1\n");
                    for(int i=0; i<thread1Iterations; i++)
                            tq.push(i);
                    printf("T1Done\n");
                    finished.set();
            });
    if (thread2Iterations)
            startThreadF([&](){
                    printf("Thread2\n");
                    for(int i=0; i<thread2Iterations; i++)
                            tq.push(i + (1<<20));
                    printf("T2Done\n");
                    finished2.set();
            });
    int c = 0, mx[2]={0, 1<<20}, p = 0;
    while (c < thread1Iterations + thread2Iterations)
    {
            Optional<int> i = tq.pop();
            if (i.present()) {
                    int v = i.get();
                    ++c;
                    if (mx[v>>20] != v)
                            printf("Wrong value dequeued!\n");
                    ASSERT( mx[v>>20] == v );
                    mx[v>>20] = v + 1;
            } else {
                    ++p;
                    _mm_pause();
            }
            if ((c&3)==0) tq.canSleep();
    }
    printf("%d %d %x %x %s\n", c, p, mx[0], mx[1], mx[0]==thread1Iterations &&
    mx[1]==(1<<20)+thread2Iterations ? "OK" : "FAIL");

    finished.block();
    finished2.block();


    g_network = newNet2(NetworkAddress::parse("127.0.0.1:12345"));  // for
    promise serialization below

    Endpoint destination;

    printf("  Used: %lld\n", FastAllocator<4096>::getMemoryUsed());

    char junk[100];

    double before = timer();

    vector<TestGVR> reqs;
    reqs.reserve( 10000 );

    int totalBytes = 0;
    for(int j=0; j<1000; j++) {
            UnsentPacketQueue unsent;
            ReliablePacketList reliable;

            reqs.resize(10000);
            for(int i=0; i<10000; i++) {
                    TestGVR &req = reqs[i];
                    req.key = LiteralStringRef("Foobar");

                    SerializeSource<TestGVR> what(req);

                    SendBuffer* pb = unsent.getWriteBuffer();
                    ReliablePacket* rp = new ReliablePacket;  // 0

                    PacketWriter
    wr(pb,rp,AssumeVersion(currentProtocolVersion));
                    //BinaryWriter wr;
                    SplitBuffer packetLen;
                    uint32_t len = 0;
                    wr.writeAhead(sizeof(len), &packetLen);
                    wr << destination.token;
                    //req.reply.getEndpoint();
                    what.serializePacketWriter(wr);
                    //wr.serializeBytes(junk, 43);

                    unsent.setWriteBuffer(wr.finish());
                    len = wr.size() - sizeof(len);
                    packetLen.write(&len, sizeof(len));

                    //totalBytes += wr.getLength();
                    totalBytes += wr.size();

                    if (rp) reliable.insert(rp);
            }
            reqs.clear();
            unsent.discardAll();
            reliable.discardAll();
    }

    printf("SimSend x 1Kx10K: %0.2f sec\n", timer()-before);
    printf("  Bytes: %d\n", totalBytes);
    printf("  Used: %lld\n", FastAllocator<4096>::getMemoryUsed());
    */
};

void*
getCurrentCoro()
{
    return N2::current_coro;
}
