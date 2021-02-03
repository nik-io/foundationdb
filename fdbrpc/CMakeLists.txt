set(FDBRPC_SRCS
  AsyncFileCached.actor.h
  AsyncFileEIO.actor.h
  AsyncFileKAIO.actor.h
  AsyncFileIOUring.actor.h
  AsyncFileNonDurable.actor.h
  AsyncFileReadAhead.actor.h
  AsyncFileWinASIO.actor.h
  AsyncFileCached.actor.cpp
  AsyncFileNonDurable.actor.cpp
  AsyncFileWriteChecker.cpp
  FailureMonitor.actor.cpp
  FlowTransport.actor.cpp
  genericactors.actor.h
  genericactors.actor.cpp
  HealthMonitor.actor.cpp
  IAsyncFile.actor.cpp
  LoadBalance.actor.h
  Locality.cpp
  Net2FileSystem.cpp
  networksender.actor.h
  QueueModel.cpp
  ReplicationPolicy.cpp
  ReplicationTypes.cpp
  ReplicationUtils.cpp
  Stats.actor.cpp
  Stats.h
  sim2.actor.cpp
  sim_validation.cpp
  TimedRequest.h
  TraceFileIO.cpp)

set(FDBRPC_THIRD_PARTY_SRCS
  libcoroutine/Common.c
  libcoroutine/Coro.c)

if(APPLE)
  list(APPEND FDBRPC_THIRD_PARTY_SRCS libcoroutine/asm.S)
endif()
if(NOT WIN32)
  if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    find_library(EIO eio)
    if(EIO)
      list(APPEND FDBRPC_THIRD_PARTY_SRCS libcoroutine/context.c)
    else()
      list(APPEND FDBRPC_THIRD_PARTY_SRCS libcoroutine/context.c libeio/eio.c)
    endif()
  else()
    list(APPEND FDBRPC_THIRD_PARTY_SRCS libcoroutine/context.c libeio/eio.c)
  endif()
endif()

add_library(thirdparty STATIC ${FDBRPC_THIRD_PARTY_SRCS})
if(NOT WIN32)
  target_compile_options(thirdparty BEFORE PRIVATE -w) # disable warnings for third party
endif()
if(USE_VALGRIND)
  target_link_libraries(thirdparty PUBLIC Valgrind)
endif()

# add_library(uring SHARED IMPORTED) # or STATIC instead of SHARED
# set_target_properties(uring PROPERTIES
#   IMPORTED_LOCATION "/home/nio/projects/liburing/src/liburing.so.2.0.0"
#   #IMPORTED_LOCATION_RELEASE "/home/nio/projects/liburing/src/liburing.a;/home/nio/projects/liburing/libasio_uring_asio.a;/home/nio/projects/liburing/libasio_uring_core.a"
#         INTERFACE_INCLUDE_DIRECTORIES "/home/nio/projects/liburing/src/include"
#         )
add_library(uring STATIC IMPORTED) # or STATIC instead of SHARED
set_target_properties(uring PROPERTIES
  IMPORTED_LOCATION "/home/nio/projects/liburing/src/liburing.a"
  #IMPORTED_LOCATION_RELEASE "/home/nio/projects/liburing/src/liburing.a;/home/nio/projects/liburing/libasio_uring_asio.a;/home/nio/projects/liburing/libasio_uring_core.a"
        INTERFACE_INCLUDE_DIRECTORIES "/home/nio/projects/liburing/src/include"
        )

add_library(asiou1 STATIC IMPORTED) # or STATIC instead of SHARED
set_target_properties(asiou1 PROPERTIES
  IMPORTED_LOCATION "/home/nio/projects/liburing/libasio_uring_asio.a"
        INTERFACE_INCLUDE_DIRECTORIES "/home/nio/projects/liburing/src/include"
        )
add_library(asiou2 STATIC IMPORTED) # or STATIC instead of SHARED
set_target_properties(asiou2 PROPERTIES
  IMPORTED_LOCATION "/home/nio/projects/liburing/libasio_uring_core.a"
        INTERFACE_INCLUDE_DIRECTORIES "/home/nio/projects/liburing/src/include"
        )

set(FDBRPC_SRCS_DISABLE_ACTOR_DIAGNOSTICS
  ActorFuzz.actor.cpp
  FlowTests.actor.cpp
  dsltest.actor.cpp)

add_flow_target(STATIC_LIBRARY NAME fdbrpc
  SRCS ${FDBRPC_SRCS}
  DISABLE_ACTOR_DIAGNOSTICS ${FDBRPC_SRCS_DISABLE_ACTOR_DIAGNOSTICS})

#target_include_directories(fdbrpc PRIVATE /root/liburing/src/include)
target_include_directories(fdbrpc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libeio)
target_link_libraries(fdbrpc PRIVATE asiou1)
target_link_libraries(fdbrpc PRIVATE asiou2)
target_link_libraries(fdbrpc PRIVATE uring)
target_link_libraries(fdbrpc PRIVATE thirdparty)
target_link_libraries(fdbrpc PUBLIC flow)
#target_link_libraries(fdbrpc PRIVATE /home/nio/projects/liburing/libasio_uring_asio.a)
#target_link_libraries(fdbrpc PRIVATE /home/nio/projects/liburing/libasio_uring_core.a)
