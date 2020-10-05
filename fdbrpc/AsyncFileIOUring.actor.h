#pragma once
#ifdef __linux__

//  When actually compiled (NO_INTELLISENSE), include the generated version of
// this file.  In intellisense use the source version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_ASYNCFILE_IO_URING_ACTOR_G_H)
#define FLOW_ASYNCFILE_IO_URING_ACTOR_G_H
#include "AsyncFileIOUring.actor.g.h"
#elif !defined(FLOW_ASYNCFILE_IO_URING_ACTOR_H)
#define FLOW_ASYNCFILE_IO_URING_ACTOR_H

#include "flow/IAsyncFile.h"
#include "flow/Trace.h"
#include "flow/network.h"
#include <liburing.h>
#include <array>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#undef FLOW_ACOMPILER_STATE
#define FLOW_ACOMPILER_STATE 1

class ISubmitUring
{
public:
    virtual void enqueue(IOUringTask* task) = 0;
};

class AsyncFileIOUring : public IAsyncFile,
                         public ReferenceCounted<AsyncFileIOUring>
{
    ISubmitUring& context;
    std::string filename;
    int fd, flags;
    int64_t lastFileSize = 0, nextFileSize = 0;
    // TODO: port timeout feature
    bool failed = false;
    Promise<Void> onClosePromise;

public:
    enum class IOOp
    {
        Read,
        Write,
        FSync
    };
    static const char* to_string(IOOp op)
    {
        switch (op) {
        case IOOp::Read:
            return "Read";
        case IOOp::Write:
            return "Write";
        case IOOp::FSync:
            return "FSync";
        default:
            return "InvalidOp";
        }
    }
    struct IOBlock : IOUringTask, FastAllocated<IOBlock>
    {
        Promise<int> result;
        Reference<AsyncFileIOUring> owner;
        IOOp operation;
        int fd;
        struct iovec iov;
        int64_t prio = 0;
        void* buf;
        int nbytes;
        int64_t offset;
        TaskId taskID{ TaskUnknown };

        IOBlock(IOOp operation, int fd)
          : fd(fd)
          , operation(operation)
        {}

        struct indirect_order_by_priority
        {
            bool operator()(IOBlock* a, IOBlock* b)
            {
                return a->prio < b->prio;
            }
        };

        void enqueue(io_uring_sqe* sqe)
        {
            iov.iov_base = buf;
            iov.iov_len = nbytes;

            switch (operation) {
            case IOOp::Read:
                io_uring_prep_readv(sqe, fd, &iov, 1, offset);
                break;
            case IOOp::Write:
                io_uring_prep_writev(sqe, fd, &iov, 1, offset);
                break;
            case IOOp::FSync:
                io_uring_prep_fsync(sqe, fd, 0);
                break;
            default:
                UNSTOPPABLE_ASSERT(false);
            }
        }

        TaskId getTask() const { return taskID.withPriorityDelta(1); }

        ACTOR static void deliver(Promise<int> result, bool failed, int r,
                                  TaskId task)
        {
            Void _uvar = wait(delay(0, task));
            if (failed)
                result.sendError(io_timeout());
            else if (r < 0)
                result.sendError(io_error());
            else
                result.send(r);
        }

        void setResult(int r)
        {
            if (r < 0) {
                struct stat fst;
                fstat(fd, &fst);

                errno = -r;
                TraceEvent("AsyncFileIOUringError")
                    .GetLastError()
                    .detail("fd", fd)
                    .detail("op", to_string(operation))
                    .detail("nbytes", nbytes)
                    .detail("offset", offset)
                    .detail("ptr", int64_t(buf))
                    .detail("Size", fst.st_size)
                    .detail("filename", owner ? owner->filename : "?");
            }
            deliver(result, owner->failed, r, getTask());
            delete this;
        }
    };

    void addref() override { ReferenceCounted<AsyncFileIOUring>::addref(); }
    void delref() override { ReferenceCounted<AsyncFileIOUring>::delref(); }

    static int openFlags(int flags)
    {
        int oflags = 0;
        ASSERT(bool(flags & OPEN_READONLY) !=
               bool(flags & OPEN_READWRITE)); // readonly xor readwrite
        if (flags & OPEN_EXCLUSIVE)
            oflags |= O_EXCL;
        if (flags & OPEN_CREATE)
            oflags |= O_CREAT;
        if (flags & OPEN_READONLY)
            oflags |= O_RDONLY;
        if (flags & OPEN_READWRITE)
            oflags |= O_RDWR;
        if (flags & OPEN_ATOMIC_WRITE_AND_CREATE)
            oflags |= O_TRUNC;
        return oflags;
    }

    static Future<Reference<IAsyncFile>> open(ISubmitUring& context,
                                              std::string filename, int flags,
                                              int mode, void* ignore)
    {
        if (flags & OPEN_LOCK)
            mode |= 02000; // Enable mandatory locking for this file if it
                           // is supported by the filesystem

        std::string open_filename = filename;
        if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
            ASSERT((flags & OPEN_CREATE) && (flags & OPEN_READWRITE) &&
                   !(flags & OPEN_EXCLUSIVE));
            open_filename = filename + ".part";
        }

        int fd =
            ::open(open_filename.c_str(), openFlags(flags) | O_DIRECT, mode);
        if (fd < 0) {
            Error e = errno == ENOENT ? file_not_found() : io_error();
            TraceEvent("AsyncFileIOUringOpenFailed")
                .detail("Filename", filename)
                .detailf("Flags", "{:x}", flags)
                .detailf("OSFlags", "{:x}", openFlags(flags) | O_DIRECT)
                .detailf("mode", "0{:o}", mode)
                .error(e)
                .GetLastError();
            return e;
        } else {
            TraceEvent("AsyncFileIOUringOpen")
                .detail("Filename", filename)
                .detail("Flags", flags)
                .detail("mode", mode)
                .detail("fd", fd);
        }

        if (flags & OPEN_LOCK) {
            // Acquire a "write" lock for the entire file
            flock lockDesc;
            lockDesc.l_type = F_WRLCK;
            lockDesc.l_whence = SEEK_SET;
            lockDesc.l_start = 0;
            lockDesc.l_len = 0; // "Specifying 0 for l_len has the special
                                // meaning: lock all bytes starting at the
                                // location specified by l_whence and
                                // l_start through to the end of file, no
                                // matter how large the file grows."
            lockDesc.l_pid = 0;
            if (fcntl(fd, F_SETLK, &lockDesc) == -1) {
                TraceEvent(SevError, "UnableToLockFile")
                    .detail("filename", filename)
                    .GetLastError();
                return io_error();
            }
        }

        Reference<AsyncFileIOUring> res(
            new AsyncFileIOUring(context, fd, flags, filename));

        struct stat buf;
        if (fstat(fd, &buf)) {
            TraceEvent("AsyncFileKAIOFStatError")
                .detail("fd", fd)
                .detail("filename", filename)
                .GetLastError();
            return io_error();
        }

        res->lastFileSize = res->nextFileSize = buf.st_size;
        return Reference<IAsyncFile>(std::move(res));
    }

    AsyncFileIOUring(ISubmitUring& context, int fd, int flags,
                     const std::string& filename)
      : context(context)
      , filename(filename)
      , fd(fd)
      , flags(flags)
    {}

    void enqueue(IOBlock* io)
    {
        io->taskID = g_network->getCurrentTask();
        io->owner = Reference<AsyncFileIOUring>::addRef(this);

        context.enqueue(io);
    }

    Future<int> read(void* data, int length, int64_t offset) override
    {
        ++g_network->networkMetrics.countFileLogicalReads;

        if (failed) {
            return io_timeout();
        }

        IOBlock* io = new IOBlock(IOOp::Read, fd);
        io->buf = data;
        io->nbytes = length;
        io->offset = offset;

        enqueue(io);
        return io->result.getFuture();
    }

    Future<Void> write(void const* data, int length, int64_t offset) override
    {
        ++g_network->networkMetrics.countFileLogicalWrites;

        if (failed) {
            return io_timeout();
        }

        IOBlock* io = new IOBlock(IOOp::Write, fd);
        io->buf = (void*)data;
        io->nbytes = length;
        io->offset = offset;

        nextFileSize = std::max(nextFileSize, offset + length);

        enqueue(io);
        return success(io->result.getFuture());
    }

    Future<Void> sync() override
    {
        ++g_network->networkMetrics.countFileLogicalWrites;

        if (failed) {
            return io_timeout();
        }

        IOBlock* io = new IOBlock(IOOp::FSync, fd);
        io->buf = nullptr;
        io->nbytes = 0;
        io->offset = 0;

        enqueue(io);
        return success(io->result.getFuture());
    }

    Future<int64_t> size() override { return nextFileSize; }

    int64_t debugFD() override { return fd; }
    Future<Void> onClose() override { return onClosePromise.getFuture(); }

    Future<Void> truncate(int64_t size) override
    {
        ++g_network->networkMetrics.countFileLogicalWrites;

        if (failed) {
            return io_timeout();
        }

        if (ftruncate(fd, size)) {
            TraceEvent("AsyncFileIOUringTruncateError")
                .detail("fd", fd)
                .detail("filename", filename)
                .GetLastError();
            return io_error();
        }
        lastFileSize = nextFileSize = size;
        return Void();
    }

    ~AsyncFileIOUring()
    {
        close(fd);
        onClosePromise.send(Void());
    }
};

#endif
#endif
#undef FLOW_ACOMPILER_STATE
#define FLOW_ACOMPILER_STATE 0