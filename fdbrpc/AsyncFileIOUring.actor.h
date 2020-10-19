/*
 * AsyncFileIOUring.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#ifdef __linux__

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_ASYNCFILEIOUring_ACTOR_G_H)
	#define FLOW_ASYNCFILEIOUring_ACTOR_G_H
	#include "fdbrpc/AsyncFileIOUring.actor.g.h"
#elif !defined(FLOW_ASYNCFILEIOUring_ACTOR_H)
	#define FLOW_ASYNCFILEIOUring_ACTOR_H

#include "fdbrpc/IAsyncFile.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <liburing.h>
//#include "fdbrpc/linux_kaio.h"
#include "flow/Knobs.h"
#include "flow/UnitTest.h"
#include <stdio.h>
#include "flow/crc32c.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

// Set this to true to enable detailed IOUring request logging, which currently is written to a hardcoded location /data/v7/fdb/
#define IOUring_LOGGING 0

enum {
	UIO_CMD_PREAD = 0,
	UIO_CMD_PWRITE = 1,
	UIO_CMD_FSYNC = 2,
	UIO_CMD_FDSYNC = 3
};

DESCR struct SlowIOUringSubmit {
	int64_t submitDuration; // ns
	int64_t truncateDuration; // ns
	int64_t numTruncates;
	int64_t truncateBytes;
	int64_t largestTruncate;
};

typedef struct io_uring io_uring_t;

class AsyncFileIOUring : public IAsyncFile, public ReferenceCounted<AsyncFileIOUring> {
public:

#if IOUring_LOGGING
private:
	#pragma pack(push, 1)
	struct OpLogEntry {
		OpLogEntry() : result(0) {}
		enum EOperation { READ = 1, WRITE = 2, SYNC = 3, TRUNCATE = 4 };
		enum EStage { START = 1, LAUNCH = 2, REQUEUE = 3, COMPLETE = 4, READY = 5 };
		int64_t timestamp;
		uint32_t id;
		uint32_t checksum;
		uint32_t pageOffset;
		uint8_t pageCount;
		uint8_t op;
		uint8_t stage;
		uint32_t result;

		static uint32_t nextID() {
			static uint32_t last = 0;
			return ++last;
		}

		void log(FILE *file) {
			if(ftell(file) > (int64_t)50 * 1e9)
				fseek(file, 0, SEEK_SET);
			if(!fwrite(this, sizeof(OpLogEntry), 1, file))
				throw io_error();
		}
	};
	#pragma pop

	FILE *logFile;
	struct IOBlock;
	static void IOUringLogBlockEvent(IOBlock *ioblock, OpLogEntry::EStage stage, uint32_t result = 0);
	static void IOUringLogBlockEvent(FILE *logFile, IOBlock *ioblock, OpLogEntry::EStage stage, uint32_t result = 0);
	static void IOUringLogEvent(FILE *logFile, uint32_t id, OpLogEntry::EOperation op, OpLogEntry::EStage stage, uint32_t pageOffset = 0, uint32_t result = 0);
public:
#else
	#define IOUringLogBlockEvent(...)
	#define IOUringLogEvent(...)
#endif

	static Future<Reference<IAsyncFile>> open( std::string filename, int flags, int mode ) {
		ASSERT( !FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO );
		/* IOUring doesn't have to be unbuffered */
		/* ASSERT( flags & OPEN_UNBUFFERED ); */

		if (flags & OPEN_LOCK)
			mode |= 02000;  // Enable mandatory locking for this file if it is supported by the filesystem

		std::string open_filename = filename;
		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
			ASSERT( (flags & OPEN_CREATE) && (flags & OPEN_READWRITE) && !(flags & OPEN_EXCLUSIVE) );
			open_filename = filename + ".part";
		}

		int fd = ::open( open_filename.c_str(), openFlags(flags), mode );
		printf("Opening %s\n",filename.c_str());
		if (fd<0) {
			Error e = errno==ENOENT ? file_not_found() : io_error();
			int ecode = errno;  // Save errno in case it is modified before it is used below
			TraceEvent ev("AsyncFileIOUringOpenFailed");
			ev.error(e).detail("Filename", filename).detailf("Flags", "%x", flags)
			  .detailf("OSFlags", "%x", openFlags(flags)).detailf("Mode", "0%o", mode).GetLastError();
			if(ecode == EINVAL)
				ev.detail("Description", "Invalid argument - Does the target filesystem support IOUring?");
			return e;
		} else {
			TraceEvent("AsyncFileIOUringOpen")
				.detail("Filename", filename)
				.detail("Flags", flags)
				.detail("Mode", mode)
				.detail("Fd", fd);
		}

		Reference<AsyncFileIOUring> r(new AsyncFileIOUring( fd, flags, filename ));

		if (flags & OPEN_LOCK) {
			// Acquire a "write" lock for the entire file
			flock lockDesc;
			lockDesc.l_type = F_WRLCK;
			lockDesc.l_whence = SEEK_SET;
			lockDesc.l_start = 0;
			lockDesc.l_len = 0;  // "Specifying 0 for l_len has the special meaning: lock all bytes starting at the location specified by l_whence and l_start through to the end of file, no matter how large the file grows."
			lockDesc.l_pid = 0;
			if (fcntl(fd, F_SETLK, &lockDesc) == -1) {
				TraceEvent(SevError, "UnableToLockFile").detail("Filename", filename).GetLastError();
				return io_error();
			}
		}

		struct stat buf;
		if (fstat( fd, &buf )) {
			TraceEvent("AsyncFileIOUringFStatError").detail("Fd",fd).detail("Filename", filename).GetLastError();
			return io_error();
		}

		r->lastFileSize = r->nextFileSize = buf.st_size;
		return Reference<IAsyncFile>(std::move(r));
	}

	static void init( Reference<IEventFD> ev, double ioTimeout ) {
		ASSERT( !FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO );
		if( !g_network->isSimulated() ) {
			ctx.countAIOSubmit.init(LiteralStringRef("AsyncFile.CountAIOSubmit"));
			ctx.countAIOCollect.init(LiteralStringRef("AsyncFile.CountAIOCollect"));
			ctx.submitMetric.init(LiteralStringRef("AsyncFile.Submit"));
			ctx.countPreSubmitTruncate.init(LiteralStringRef("AsyncFile.CountPreAIOSubmitTruncate"));
			ctx.preSubmitTruncateBytes.init(LiteralStringRef("AsyncFile.PreAIOSubmitTruncateBytes"));
			ctx.slowAioSubmitMetric.init(LiteralStringRef("AsyncFile.SlowIOUringSubmit"));
		}
		printf("Initing iouring\n");
		int rc = io_uring_queue_init(FLOW_KNOBS->MAX_OUTSTANDING, &ctx.ring, 0);
		printf("Inited iouring with rc %d. evfd %d\n",rc,ev->getFD());
		//int rc = io_setup( FLOW_KNOBS->MAX_OUTSTANDING, &ctx.iocx );
		if (rc<0) {
			TraceEvent("IOSetupError").GetLastError();
			throw io_error();
		}
		setTimeout(ioTimeout);
		ctx.evfd = ev->getFD();
		poll(ev);

		g_network->setGlobal(INetwork::enRunCycleFunc, (flowGlobalType) &AsyncFileIOUring::launch);
	}

	static int get_eventfd() { return ctx.evfd; }
	static void setTimeout(double ioTimeout) { ctx.setIOTimeout(ioTimeout); }

	virtual void addref() { ReferenceCounted<AsyncFileIOUring>::addref(); }
	virtual void delref() { ReferenceCounted<AsyncFileIOUring>::delref(); }

	Future<int> read(void* data, int length, int64_t offset) override {
		++countFileLogicalReads;
		++countLogicalReads;
		printf("Begin logical read %s\n", filename.c_str());

		if(failed) {
			return io_timeout();
		}

		IOBlock *io = new IOBlock(UIO_CMD_PREAD, fd);
		io->buf = data;
		io->nbytes = length;
		io->offset = offset;

		enqueue(io, "read", this);
		Future<int> result = io->result.getFuture();

#if IOUring_LOGGING
		//result = map(result, [=](int r) mutable { IOUringLogBlockEvent(io, OpLogEntry::READY, r); return r; });
#endif

		return result;
	}
	Future<Void> write(void const* data, int length, int64_t offset) override {
		++countFileLogicalWrites;
		++countLogicalWrites;
		printf("Begin logical write on %s\n", filename.c_str());

		if(failed) {
			return io_timeout();
		}

		IOBlock *io = new IOBlock(UIO_CMD_PWRITE, fd);
		io->buf = (void*)data;
		io->nbytes = length;
		io->offset = offset;

		nextFileSize = std::max( nextFileSize, offset+length );

		enqueue(io, "write", this);
		Future<int> result = io->result.getFuture();
		
#if IOUring_LOGGING
		//result = map(result, [=](int r) mutable { IOUringLogBlockEvent(io, OpLogEntry::READY, r); return r; });
#endif

		return success(result);
	}
// TODO(alexmiller): Remove when we upgrade the dev docker image to >14.10
#ifndef FALLOC_FL_ZERO_RANGE
#define FALLOC_FL_ZERO_RANGE 0x10
#endif
	Future<Void> zeroRange(int64_t offset, int64_t length) override {
		bool success = false;
		if (ctx.fallocateZeroSupported) {
			int rc = fallocate( fd, FALLOC_FL_ZERO_RANGE, offset, length );
			if (rc == EOPNOTSUPP) {
				ctx.fallocateZeroSupported = false;
			}
			if (rc == 0) {
				success = true;
			}
		}
		return success ? Void() : IAsyncFile::zeroRange(offset, length);
	}
	Future<Void> truncate(int64_t size) override {
		++countFileLogicalWrites;
		++countLogicalWrites;

		if(failed) {
			return io_timeout();
		}

#if IOUring_LOGGING
		uint32_t id = OpLogEntry::nextID();
#endif
		int result = -1;
		IOUringLogEvent(logFile, id, OpLogEntry::TRUNCATE, OpLogEntry::START, size / 4096);
		bool completed = false;
		double begin = timer_monotonic();

		if( ctx.fallocateSupported && size >= lastFileSize ) {
			result = fallocate( fd, 0, 0, size);
			if (result != 0) {
				int fallocateErrCode = errno;
				TraceEvent("AsyncFileIOUringAllocateError").detail("Fd",fd).detail("Filename", filename).detail("Size", size).GetLastError();
				if ( fallocateErrCode == EOPNOTSUPP ) {
					// Mark fallocate as unsupported. Try again with truncate.
					ctx.fallocateSupported = false;
				} else {
					IOUringLogEvent(logFile, id, OpLogEntry::TRUNCATE, OpLogEntry::COMPLETE, size / 4096, result);
					return io_error();
				}
			} else {
				completed = true;
			}
		}
		if ( !completed )
			result = ftruncate(fd, size);

		double end = timer_monotonic();
		if(nondeterministicRandom()->random01() < end-begin) {
			TraceEvent("SlowIOUringTruncate")
				.detail("TruncateTime", end - begin)
				.detail("TruncateBytes", size - lastFileSize);
		}
		IOUringLogEvent(logFile, id, OpLogEntry::TRUNCATE, OpLogEntry::COMPLETE, size / 4096, result);

		if(result != 0) {
			TraceEvent("AsyncFileIOUringTruncateError").detail("Fd",fd).detail("Filename", filename).GetLastError();
			return io_error();
		}

		lastFileSize = nextFileSize = size;

		return Void();
	}

	ACTOR static Future<Void> throwErrorIfFailed( Reference<AsyncFileIOUring> self, Future<Void> sync ) {
		wait( sync );
		if(self->failed) {
			throw io_timeout();
		}
		return Void();
	}

	Future<Void> sync() override {
	    printf("Begin logical fsync on %s\n",filename.c_str());
		++countFileLogicalWrites;
		++countLogicalWrites;

		if(failed) {
			return io_timeout();
		}

#if IOUring_LOGGING
		uint32_t id = OpLogEntry::nextID();
#endif

		IOUringLogEvent(logFile, id, OpLogEntry::SYNC, OpLogEntry::START);



		Future<Void> fsync = throwErrorIfFailed(Reference<AsyncFileIOUring>::addRef(this), AsyncFileEIO::async_fdatasync(fd));  // Don't close the file until the asynchronous thing is done
		// Alas, AIO f(data)sync doesn't seem to actually be implemented by the kernel
		/*IOBlock *io = new IOBlock(UIO_CMD_FDSYNC, fd);
		  submit(io, "write");
		  fsync=success(io->result.getFuture());*/

		/* TODO */
		/* struct io_uring_sqe *sqe = io_uring_get_sqe(&ctx.ring); */
		/* io_uring_prep_fsync(sqe, this->fd, ORING_FSYNC_DATASYNC); */

#if IOUring_LOGGING
		fsync = map(fsync, [=](Void r) mutable { IOUringLogEvent(logFile, id, OpLogEntry::SYNC, OpLogEntry::COMPLETE); return r; });
#endif

		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
			flags &= ~OPEN_ATOMIC_WRITE_AND_CREATE;
            printf("End sync ATOMIC_CREATE\n");
			return AsyncFileEIO::waitAndAtomicRename( fsync, filename+".part", filename );
		}
        printf("End sync with\n");
		return fsync;
	}
	Future<int64_t> size() const override { return nextFileSize; }
	int64_t debugFD() const override { return fd; }
	std::string getFilename() const override { return filename; }
	~AsyncFileIOUring() {
		close(fd);

#if IOUring_LOGGING
		if(logFile != nullptr)
			fclose(logFile);
#endif
	}

	static void launch1() {
		printf("Launch on %p. Outstanding %d enqueued %d\n",&ctx,ctx.outstanding, ctx.queue.size());
		if (ctx.queue.size() && ctx.outstanding < FLOW_KNOBS->MAX_OUTSTANDING - FLOW_KNOBS->MIN_SUBMIT) {
			printf("entering launch if\n");
			ctx.submitMetric = true;

			double begin = timer_monotonic();
			if (!ctx.outstanding) ctx.ioStallBegin = begin;

			IOBlock* toStart[FLOW_KNOBS->MAX_OUTSTANDING];
			int n = std::min<size_t>(FLOW_KNOBS->MAX_OUTSTANDING - ctx.outstanding, ctx.queue.size());
			printf("%d events in queue. Outstanding %d max %d \n",ctx.queue.size(), ctx.outstanding, FLOW_KNOBS->MAX_OUTSTANDING,n);
			int64_t previousTruncateCount = ctx.countPreSubmitTruncate;
			int64_t previousTruncateBytes = ctx.preSubmitTruncateBytes;
			int64_t largestTruncate = 0;
			int dequeued_nr = 0;
			int i=0;
			for(; i<n; i++) {
				auto io = ctx.queue.top();
//				int rc = 0;
				toStart[i] = io;
				io->startTime = now();
				struct io_uring_sqe *sqe = io_uring_get_sqe(&ctx.ring);
				if (nullptr == sqe)
					break;

				IOUringLogBlockEvent(io, OpLogEntry::LAUNCH);

				//prep does not return error code
				switch(io->opcode){
				case UIO_CMD_PREAD:
				    io_uring_prep_read(sqe, io->aio_fildes,  io->buf, io->nbytes, io->offset);
				        break;
				case UIO_CMD_PWRITE:
					io_uring_prep_write(sqe, io->aio_fildes,  io->buf, io->nbytes, io->offset);
				        break;
				case UIO_CMD_FSYNC:
				     io_uring_prep_fsync(sqe, io->aio_fildes, 0);
				default:
					UNSTOPPABLE_ASSERT(false);
				}
				if (1) {//prep never fails
					/* pop only iff pushed at the kernel */
					ctx.queue.pop();
				} else {
					/* TODO: consider breaking */
				}

				io_uring_sqe_set_data(sqe, &io);
				if(ctx.ioTimeout > 0) {
					ctx.appendToRequestList(io);
				}

				if (io->owner->lastFileSize != io->owner->nextFileSize) {
					++ctx.countPreSubmitTruncate;
					int64_t truncateSize = io->owner->nextFileSize - io->owner->lastFileSize;
					ASSERT(truncateSize > 0);
					ctx.preSubmitTruncateBytes += truncateSize;
					largestTruncate = std::max(largestTruncate, truncateSize);
					io->owner->truncate(io->owner->nextFileSize);
				}
			}
			dequeued_nr = i;

			double truncateComplete = timer_monotonic();
			int rc = io_uring_submit(&ctx.ring);
			/* int rc = io_submit( ctx.iocx, n, (linux_iocb**)toStart ); */
			double end = timer_monotonic();
			if (0 <= rc)
				printf("io_uring_submit submitted %d items\n", rc);
			else
				printf("io_uring_submit error %d %s\n", rc, strerror(-rc));

			//Thre might be unpushed items. These have been prepped already, and the corresponding sqe
			//should be already ready to be pushed next time

			if(end-begin > FLOW_KNOBS->SLOW_LOOP_CUTOFF) {
				ctx.slowAioSubmitMetric->submitDuration = end-truncateComplete;
				ctx.slowAioSubmitMetric->truncateDuration = truncateComplete-begin;
				ctx.slowAioSubmitMetric->numTruncates = ctx.countPreSubmitTruncate - previousTruncateCount;
				ctx.slowAioSubmitMetric->truncateBytes = ctx.preSubmitTruncateBytes - previousTruncateBytes;
				ctx.slowAioSubmitMetric->largestTruncate = largestTruncate;
				ctx.slowAioSubmitMetric->log();

				if(nondeterministicRandom()->random01() < end-begin) {
					TraceEvent("SlowIOUringLaunch")
						.detail("IOSubmitTime", end-truncateComplete)
						.detail("TruncateTime", truncateComplete-begin)
						.detail("TruncateCount", ctx.countPreSubmitTruncate - previousTruncateCount)
						.detail("TruncateBytes", ctx.preSubmitTruncateBytes - previousTruncateBytes)
						.detail("LargestTruncate", largestTruncate);
				}
			}

			ctx.submitMetric = false;
			++ctx.countAIOSubmit;

			double elapsed = timer_monotonic() - begin;
			g_network->networkInfo.metrics.secSquaredSubmit += elapsed*elapsed/2;

			//TraceEvent("Launched").detail("N", rc).detail("Queued", ctx.queue.size()).detail("Elapsed", elapsed).detail("Outstanding", ctx.outstanding+rc);
			//printf("launched: %d/%d in %f us (%d outstanding; lowest prio %d)\n", rc, ctx.queue.size(), elapsed*1e6, ctx.outstanding + rc, toStart[n-1]->getTask());
			if (rc<0) {
				if (errno == EAGAIN) {
					rc = 0;
				} else {
					IOUringLogBlockEvent(toStart[0], OpLogEntry::COMPLETE, errno ? -errno : -1000000);
					// Other errors are assumed to represent failure to issue the first I/O in the list
					toStart[0]->setResult( errno ? -errno : -1000000 );
					rc = 1;
				}
			} else{
				ctx.outstanding += rc;
				}
		}
		printf("out of launch\n");
	}

	static void launch() {
		printf("Launch on %p. Outstanding %d enqueued %d\n",&ctx,ctx.outstanding, ctx.queue.size());
		//FOr now, don-t worry about min submit
		//We want to get to call "submit" if we have stuff in the ctx queue or in the ring queue
		if (ctx.queue.size() + ctx.outstanding < FLOW_KNOBS->MAX_OUTSTANDING) {
			printf("entering launch if\n");
			ctx.submitMetric = true;

			double begin = timer_monotonic();
			if (!ctx.outstanding) ctx.ioStallBegin = begin;

			IOBlock* toStart[FLOW_KNOBS->MAX_OUTSTANDING];
			//we only push new stuff from the ctx. Outstanding are alrady in the ring
			//(A workaround could be possibly cqe-see the outstanding and re-enqueue them, but I guess it costs too much
			int n = ctx.queue.size();
			printf("%d events in queue. Outstanding %d max %d  N= \n",ctx.queue.size(), ctx.outstanding, FLOW_KNOBS->MAX_OUTSTANDING,n);
			int64_t previousTruncateCount = ctx.countPreSubmitTruncate;
			int64_t previousTruncateBytes = ctx.preSubmitTruncateBytes;
			int64_t largestTruncate = 0;
			int dequeued_nr = 0;
			int i=0;
			for(; i<n; i++) {
				auto io = ctx.queue.top();
//				int rc = 0;
				toStart[i] = io;
				io->startTime = now();
				struct io_uring_sqe *sqe = io_uring_get_sqe(&ctx.ring);
				if (nullptr == sqe)
					break;

				IOUringLogBlockEvent(io, OpLogEntry::LAUNCH);

				//prep does not return error code
				switch(io->opcode){
				case UIO_CMD_PREAD:
				    io_uring_prep_read(sqe, io->aio_fildes,  io->buf, io->nbytes, io->offset);
				        break;
				case UIO_CMD_PWRITE:
					io_uring_prep_write(sqe, io->aio_fildes,  io->buf, io->nbytes, io->offset);
				        break;
				case UIO_CMD_FSYNC:
				     io_uring_prep_fsync(sqe, io->aio_fildes, 0);
				default:
					UNSTOPPABLE_ASSERT(false);
				}
				if (1) {//prep never fails
					/* pop only iff pushed at the kernel */
					ctx.queue.pop();
				} else {
					/* TODO: consider breaking */
				}

				io_uring_sqe_set_data(sqe, &io);
				if(ctx.ioTimeout > 0) {
					ctx.appendToRequestList(io);
				}

				if (io->owner->lastFileSize != io->owner->nextFileSize) {
					++ctx.countPreSubmitTruncate;
					int64_t truncateSize = io->owner->nextFileSize - io->owner->lastFileSize;
					ASSERT(truncateSize > 0);
					ctx.preSubmitTruncateBytes += truncateSize;
					largestTruncate = std::max(largestTruncate, truncateSize);
					io->owner->truncate(io->owner->nextFileSize);
				}
			}
			dequeued_nr = i;

			double truncateComplete = timer_monotonic();
			int rc = io_uring_submit(&ctx.ring);
			/* int rc = io_submit( ctx.iocx, n, (linux_iocb**)toStart ); */
			double end = timer_monotonic();
			if (0 <= rc)
				printf("io_uring_submit submitted %d items\n", rc);
			else
				printf("io_uring_submit error %d %s\n", rc, strerror(-rc));

			//Thre might be unpushed items. These have been prepped already, and the corresponding sqe
			//should be already ready to be pushed next time

			if(end-begin > FLOW_KNOBS->SLOW_LOOP_CUTOFF) {
				ctx.slowAioSubmitMetric->submitDuration = end-truncateComplete;
				ctx.slowAioSubmitMetric->truncateDuration = truncateComplete-begin;
				ctx.slowAioSubmitMetric->numTruncates = ctx.countPreSubmitTruncate - previousTruncateCount;
				ctx.slowAioSubmitMetric->truncateBytes = ctx.preSubmitTruncateBytes - previousTruncateBytes;
				ctx.slowAioSubmitMetric->largestTruncate = largestTruncate;
				ctx.slowAioSubmitMetric->log();

				if(nondeterministicRandom()->random01() < end-begin) {
					TraceEvent("SlowIOUringLaunch")
						.detail("IOSubmitTime", end-truncateComplete)
						.detail("TruncateTime", truncateComplete-begin)
						.detail("TruncateCount", ctx.countPreSubmitTruncate - previousTruncateCount)
						.detail("TruncateBytes", ctx.preSubmitTruncateBytes - previousTruncateBytes)
						.detail("LargestTruncate", largestTruncate);
				}
			}

			ctx.submitMetric = false;
			++ctx.countAIOSubmit;

			double elapsed = timer_monotonic() - begin;
			g_network->networkInfo.metrics.secSquaredSubmit += elapsed*elapsed/2;

			//TraceEvent("Launched").detail("N", rc).detail("Queued", ctx.queue.size()).detail("Elapsed", elapsed).detail("Outstanding", ctx.outstanding+rc);
			//printf("launched: %d/%d in %f us (%d outstanding; lowest prio %d)\n", rc, ctx.queue.size(), elapsed*1e6, ctx.outstanding + rc, toStart[n-1]->getTask());
			if (rc<0) {
				if (errno == EAGAIN) {
					rc = 0;
				} else {
					IOUringLogBlockEvent(toStart[0], OpLogEntry::COMPLETE, errno ? -errno : -1000000);
					// Other errors are assumed to represent failure to issue the first I/O in the list
					toStart[0]->setResult( errno ? -errno : -1000000 );
					rc = 1;
				}
			} else{
				ctx.outstanding += rc;
			}
		}
		printf("out of launch\n");
	}

	bool failed;
private:
	int fd, flags;
	int64_t lastFileSize, nextFileSize;
	std::string filename;
	Int64MetricHandle countFileLogicalWrites;
	Int64MetricHandle countFileLogicalReads;

	Int64MetricHandle countLogicalWrites;
	Int64MetricHandle countLogicalReads;

	struct IOBlock : FastAllocated<IOBlock> {
		Promise<int> result;
		Reference<AsyncFileIOUring> owner;
		int opcode;
		int64_t prio;
		int aio_fildes;
		void *buf;
		int64_t nbytes;
		int64_t offset;
		IOBlock *prev;
		IOBlock *next;

		double startTime;
#if IOUring_LOGGING
		int32_t iolog_id;
#endif

		struct indirect_order_by_priority { bool operator () ( IOBlock* a, IOBlock* b ) { return a->prio < b->prio; } };

		IOBlock(int op, int fd) : opcode(op), prio(0), aio_fildes(fd), buf(nullptr), nbytes(0), offset(0) {
#if IOUring_LOGGING
			iolog_id = 0;
#endif
		}

		TaskPriority getTask() const { return static_cast<TaskPriority>((prio>>32)+1); }

		ACTOR static void deliver( Promise<int> result, bool failed, int r, TaskPriority task ) {
			wait( delay(0, task) );
			if (failed) result.sendError(io_timeout());
			else if (r < 0) result.sendError(io_error());
			else result.send(r);
		}

		void setResult( int r ) {
			if (r<0) {
				struct stat fst;
				fstat( aio_fildes, &fst );

				errno = -r;
				TraceEvent("AsyncFileIOUringIOError").GetLastError().detail("Fd", aio_fildes).detail("Op", opcode).detail("Nbytes", nbytes).detail("Offset", offset).detail("Ptr", int64_t(buf))
					.detail("Size", fst.st_size).detail("Filename", owner->filename);
			}
			deliver( result, owner->failed, r, getTask() );
			delete this;
		}

		void timeout(bool warnOnly) {
			TraceEvent(SevWarnAlways, "AsyncFileIOUringTimeout").detail("Fd", aio_fildes).detail("Op", opcode).detail("Nbytes", nbytes).detail("Offset", offset).detail("Ptr", int64_t(buf))
				.detail("Filename", owner->filename);
			g_network->setGlobal(INetwork::enASIOTimedOut, (flowGlobalType)true);

			if(!warnOnly)
				owner->failed = true;
		}
	};

	struct Context {
		io_uring_t ring;
		/* io_context_t iocx; */
		int evfd;
		int outstanding;
		double ioStallBegin;
		bool fallocateSupported;
		bool fallocateZeroSupported;
		std::priority_queue<IOBlock*, std::vector<IOBlock*>, IOBlock::indirect_order_by_priority> queue;
		Int64MetricHandle countAIOSubmit;
		Int64MetricHandle countAIOCollect;
		Int64MetricHandle submitMetric;

		double ioTimeout;
		bool timeoutWarnOnly;
		IOBlock *submittedRequestList;

		Int64MetricHandle countPreSubmitTruncate;
		Int64MetricHandle preSubmitTruncateBytes;

		EventMetricHandle<SlowIOUringSubmit> slowAioSubmitMetric;

		uint32_t opsIssued;
		Context() : ring(), evfd(-1), outstanding(0), opsIssued(0), ioStallBegin(0), fallocateSupported(true), fallocateZeroSupported(true), submittedRequestList(nullptr) {
			setIOTimeout(0);
		}

		void setIOTimeout(double timeout) {
			ioTimeout = fabs(timeout);
			timeoutWarnOnly = timeout < 0;
		}

		void appendToRequestList(IOBlock *io) {
			ASSERT(!io->next && !io->prev);

			if(submittedRequestList) {
				io->prev = submittedRequestList->prev;
				io->prev->next = io;

				submittedRequestList->prev = io;
				io->next = submittedRequestList;
			}
			else {
				submittedRequestList = io;
				io->next = io->prev = io;
			}
		}

		void removeFromRequestList(IOBlock *io) {
			if(io->next == nullptr) {
				ASSERT(io->prev == nullptr);
				return;
			}

			ASSERT(io->prev != nullptr);

			if(io == io->next) {
				ASSERT(io == submittedRequestList && io == io->prev);
				submittedRequestList = nullptr;
			}
			else {
				io->next->prev = io->prev;
				io->prev->next = io->next;

				if(submittedRequestList == io) {
					submittedRequestList = io->next;
				}
			}

			io->next = io->prev = nullptr;
		}
	};
	static Context ctx;

	explicit AsyncFileIOUring(int fd, int flags, std::string const& filename) : fd(fd), flags(flags), filename(filename), failed(false) {
		ASSERT( !FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO );
		if( !g_network->isSimulated() ) {
			countFileLogicalWrites.init(LiteralStringRef("AsyncFile.CountFileLogicalWrites"), filename);
			countFileLogicalReads.init( LiteralStringRef("AsyncFile.CountFileLogicalReads"), filename);
			countLogicalWrites.init(LiteralStringRef("AsyncFile.CountLogicalWrites"));
			countLogicalReads.init( LiteralStringRef("AsyncFile.CountLogicalReads"));
		}
		
#if IOUring_LOGGING
		logFile = nullptr;
		// TODO:  Don't do this hacky investigation-specific thing
		StringRef fname(filename);
		if(fname.endsWith(LiteralStringRef(".sqlite")) || fname.endsWith(LiteralStringRef(".sqlite-wal"))) {
			std::string logFileName = basename(filename);
			while(logFileName.find("/") != std::string::npos)
				logFileName = logFileName.substr(logFileName.find("/") + 1);
			if(!logFileName.empty()) {
				// TODO: don't hardcode this path
				std::string logPath("/data/v7/fdb/");
				try {
					platform::createDirectory(logPath);
					logFileName = logPath + format("%s.iolog", logFileName.c_str());
					logFile = fopen(logFileName.c_str(), "r+");
					if(logFile == nullptr)
						logFile = fopen(logFileName.c_str(), "w");
					if(logFile != nullptr)
						TraceEvent("IOUringLogOpened").detail("File", filename).detail("LogFile", logFileName);
					else {
						TraceEvent(SevWarn, "IOUringLogOpenFailure")
							.detail("File", filename)
							.detail("LogFile", logFileName)
							.detail("ErrorCode", errno)
							.detail("ErrorDesc", strerror(errno));
					}
				} catch(Error &e) {
					TraceEvent(SevError, "IOUringLogOpenFailure").error(e);
				}
			}
		}
#endif
	}

	void enqueue( IOBlock* io, const char* op, AsyncFileIOUring* owner ) {
		printf("URING enquein data size %lu for op %s. Uncached is %d\n",int64_t(io->buf),op,bool(flags & IAsyncFile::OPEN_UNCACHED));
		ASSERT( !bool(flags & IAsyncFile::OPEN_UNCACHED) || int64_t(io->buf) % 4096 == 0);
		ASSERT(io->offset % 4096 == 0);
		ASSERT( !bool(flags & IAsyncFile::OPEN_UNCACHED) ||io->nbytes % 4096 == 0 );

		IOUringLogBlockEvent(owner->logFile, io, OpLogEntry::START);

		//io->flags |= 1;
		//io->eventfd = ctx.evfd;
		io->prio = (int64_t(g_network->getCurrentTask())<<32) - (++ctx.opsIssued);
		//io->prio = - (++ctx.opsIssued);
		io->owner = Reference<AsyncFileIOUring>::addRef(owner);

		ctx.queue.push(io);
		printf("Enqueued op %s on ctx %p\n",op,&ctx);
	}

	static int openFlags(int flags) {
		int oflags = O_CLOEXEC;
		if (flags & OPEN_UNBUFFERED) oflags |= O_DIRECT;
		ASSERT( bool(flags & OPEN_READONLY) != bool(flags & OPEN_READWRITE) );  // readonly xor readwrite
		if( flags & OPEN_EXCLUSIVE ) oflags |= O_EXCL;
		if( flags & OPEN_CREATE )    oflags |= O_CREAT;
		if( flags & OPEN_READONLY )  oflags |= O_RDONLY;
		if( flags & OPEN_READWRITE ) oflags |= O_RDWR;
		if( flags & OPEN_ATOMIC_WRITE_AND_CREATE ) oflags |= O_TRUNC;
		return oflags;
	}

	ACTOR static void poll( Reference<IEventFD> ev ) {
		loop {
			//Maybe if we do not wait on ev, we do wait_cqe and this blocks
			//The read may return a future when there is stuff to read.
			//We can implement the same thing using the peek
			/* wait(success(ev->read())); */
			/* wait(delay(0, TaskPriority::DiskIOComplete)); */

			wait(delay(0, TaskPriority::DiskIOComplete));
			printf("POLLING\n");
			struct io_uring_cqe *cqe;
			int rc = io_uring_peek_cqe(&ctx.ring, &cqe);

			//int rc = io_uring_wait_cqe(&ctx.ring, &cqe);
			printf("POLLED with rc %d %s\n",rc,strerror(-rc));

			if (rc < 0) {
			    if(rc != -EAGAIN){
				    printf("io_uring_wait_cqe failed: %d %s\n", rc, strerror(-rc));
				    TraceEvent("IOGetEventsError").GetLastError();
				    throw io_error();
			    }else{
				    printf("io_uring_peek_cqe found nothing with rc %d %s\n",rc,strerror(-rc));
				    continue;
			    }
			}

			if(!cqe){
				//Not sure if this is ever executed
			    printf("io_uring_peek_cqe found nothing with rc %d\n",rc);
			    continue;
			}
			++ctx.countAIOCollect;
			int res = cqe->res;
			/*
			 //Even if the inner call has failed, let's report it to the upper layer
			if (res < 0) {
				printf("io_uring_peek_cqe returned res: %d %s\n", cqe->res, strerror(-cqe->res));
				// The system call invoked asynchonously failed
				io_uring_cqe_seen(&ctx.ring, cqe);
				continue;
			}
			*/
			IOBlock* iob = static_cast<IOBlock*>(io_uring_cqe_get_data(cqe));
			io_uring_cqe_seen(&ctx.ring, cqe);
			cqe=nullptr;
			{
				double t = timer_monotonic();
				double elapsed = t - ctx.ioStallBegin;
				ctx.ioStallBegin = t;
				g_network->networkInfo.metrics.secSquaredDiskStall += elapsed*elapsed/2;
			}

			ctx.outstanding --;

			if(ctx.ioTimeout > 0) {
				double currentTime = now();
				while(ctx.submittedRequestList && currentTime - ctx.submittedRequestList->startTime > ctx.ioTimeout) {
					ctx.submittedRequestList->timeout(ctx.timeoutWarnOnly);
					ctx.removeFromRequestList(ctx.submittedRequestList);
				}
			}


			IOUringLogBlockEvent(iob, OpLogEntry::COMPLETE, res);

			if(ctx.ioTimeout > 0) {
				ctx.removeFromRequestList(iob);
			}

			iob->setResult(res);
		}
	}
};

#if IOUring_LOGGING
// Call from contexts where only an ioblock is available, log if its owner is set
void AsyncFileIOUring::IOUringLogBlockEvent(IOBlock *ioblock, OpLogEntry::EStage stage, uint32_t result) {
	if(ioblock->owner)
		return IOUringLogBlockEvent(ioblock->owner->logFile, ioblock, stage, result);
}

void AsyncFileIOUring::IOUringLogBlockEvent(FILE *logFile, IOBlock *ioblock, OpLogEntry::EStage stage, uint32_t result) {
	if(logFile != nullptr) {
		// Figure out what type of operation this is
		OpLogEntry::EOperation op;
		if(ioblock->opcode == UIO_CMD_PREAD)
			op = OpLogEntry::READ;
		else if(ioblock->opcode == UIO_CMD_PWRITE)
			op = OpLogEntry::WRITE;
		else
			return;

		// Assign this IO operation an io log id number if it doesn't already have one
		if(ioblock->iolog_id == 0)
			ioblock->iolog_id = OpLogEntry::nextID();

		OpLogEntry e;
		e.timestamp = timer_int();
		e.op = (uint8_t)op;
		e.id = ioblock->iolog_id;
		e.stage = (uint8_t)stage;
		e.pageOffset = (uint32_t)(ioblock->offset / 4096);
		e.pageCount = (uint8_t)(ioblock->nbytes / 4096);
		e.result = result;

		// Log a checksum for Writes up to the Complete stage or Reads starting from the Complete stage
		if( (op == OpLogEntry::WRITE && stage <= OpLogEntry::COMPLETE) || (op == OpLogEntry::READ && stage >= OpLogEntry::COMPLETE) )
			e.checksum = crc32c_append(0xab12fd93, ioblock->buf, ioblock->nbytes);
		else
			e.checksum = 0;

		e.log(logFile);
	}
}

void AsyncFileIOUring::IOUringLogEvent(FILE *logFile, uint32_t id, OpLogEntry::EOperation op, OpLogEntry::EStage stage, uint32_t pageOffset, uint32_t result) {
	if(logFile != nullptr) {
		OpLogEntry e;
		e.timestamp = timer_int();
		e.id = id;
		e.op = (uint8_t)op;
		e.stage = (uint8_t)stage;
		e.pageOffset = pageOffset;
		e.pageCount = 0;
		e.checksum = 0;
		e.result = result;
		e.log(logFile);
	}
}
#endif

ACTOR Future<Void> runTestIOUringOps(Reference<IAsyncFile> f, int numIterations, int fileSize, bool expectedToSucceed) {
	state void *buf = FastAllocator<4096>::allocate(); // we leak this if there is an error, but that shouldn't be a big deal
	state int iteration = 0;

	state bool opTimedOut = false;

	for(; iteration < numIterations; ++iteration) {
		state std::vector<Future<Void>> futures;
		state int numOps = deterministicRandom()->randomInt(1, 20);
		for(; numOps > 0; --numOps) {
			if(deterministicRandom()->coinflip()) {
				futures.push_back(success(f->read(buf, 4096, deterministicRandom()->randomInt(0, fileSize)/4096*4096)));
			}
			else {
				futures.push_back(f->write(buf, 4096, deterministicRandom()->randomInt(0, fileSize)/4096*4096));
			}
		}
		state int fIndex = 0;
		for(; fIndex < futures.size(); ++fIndex) {
			try {
				wait(futures[fIndex]);
			}
			catch(Error &e) {
				ASSERT(!expectedToSucceed);
				ASSERT(e.code() == error_code_io_timeout);
				opTimedOut = true;
			}
		}

		try {
			wait(f->sync() && delay(0.1));
			ASSERT(expectedToSucceed);
		}
		catch(Error &e) {
			ASSERT(!expectedToSucceed && e.code() == error_code_io_timeout);
		}
	}
	
	FastAllocator<4096>::release(buf);

	ASSERT(expectedToSucceed || opTimedOut);
	return Void();
}

TEST_CASE("/fdbrpc/AsyncFileIOUring/RequestList") {
	// This test does nothing in simulation because simulation doesn't support AsyncFileIOUring
	if (!g_network->isSimulated()) {
		state Reference<IAsyncFile> f;
		try {
			Reference<IAsyncFile> f_ = wait(AsyncFileIOUring::open(
			    "/tmp/__IOUring_TEST_FILE__",
			    IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE, 0666));
			f = f_;
			state int fileSize = 2 << 27; // ~100MB
			wait(f->truncate(fileSize));

			// Test that the request list works as intended with default timeout
			AsyncFileIOUring::setTimeout(0.0);
			wait(runTestIOUringOps(f, 100, fileSize, true));
			ASSERT(!((AsyncFileIOUring*)f.getPtr())->failed);

			// Test that the request list works as intended with long timeout
			AsyncFileIOUring::setTimeout(20.0);
			wait(runTestIOUringOps(f, 100, fileSize, true));
			ASSERT(!((AsyncFileIOUring*)f.getPtr())->failed);

			// Test that requests timeout correctly
			AsyncFileIOUring::setTimeout(0.0001);
			wait(runTestIOUringOps(f, 10, fileSize, false));
			ASSERT(((AsyncFileIOUring*)f.getPtr())->failed);
		} catch (Error& e) {
			state Error err = e;
			if(f) {
				wait(AsyncFileEIO::deleteFile(f->getFilename(), true));
			}
			throw err;
		}

		wait(AsyncFileEIO::deleteFile(f->getFilename(), true));
	}

	return Void();
}

AsyncFileIOUring::Context AsyncFileIOUring::ctx;

#include "flow/unactorcompiler.h"
#endif 
#endif
