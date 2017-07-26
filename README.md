# Structured Log Daemon (slogd)

slogd is a TCP-accessible, low-resource, Kafka-like log daemon built with protocol buffers
as a first-class citizen.

slogd aims to be a Kafka-like tool for the enthusiast developer, without the investment of
running Zookeeper and Kafka on a single node. While the JVM can be tuned for lower resources,
it will always be handily beat by a well-written Go program. slogd runs in approximately 14kb
of memory.

There are a lot of message daemons out there. There's servers that accept logs. But everything 
is very heavy for the enthusiast developer working on small projects.

The Kafka model is very nice: efficient pubsub semantics, multi-day retention, efficient leveraging
of Linux filesystem caches. Kafka takes advantage of kernel primitives such as sendfile(). It's
built for speed, reliability, and (generally) absolute resource utilization of a given machine.

slogd takes some of the best elements of Kafka and scales them down. Topics do not have partitions, you
probably shouldn't be pushing slogd so hard as to need partitioned behavior. At the same time, 
it's quite nice to have monotonically increasing message identifiers and write-ordered delivery.
slogd eschews the custom Kafka protocol (efficient as it may be) and implements its own protocol
built on top of gRPC and protocol buffers, while retaining a HTTP/JSON interface for non-RPC
clients.

I built slogd as a way to have Kafka-like semantics for a crawl/transform/notify infrastructure
previously running on top of hacked-together IPC via Redis. A Ruby daemon processed tasks
received via Redis, published from inside of Python in a gloriously hacky manner.

Additionally, slogd powers a custom analytics stack.

## Why should I use slogd?

You probably shouldn't yet. It's a "scratch an itch" kind of project. It might develop into more
in the future, but there's already bulletproof infrastructure written by the true professionals,
aka the Kafka team.

## Building

1) First get [dep](https://github.com/golang/dep) from Github.
2) Then you can run `dep ensure` in the slogd directory to vendor all dependencies at the versions specified.
3) You'll also need to download protobuf 3. There's a handy script under vendor/github.com/gogo/protobuf to install protobuf.
4) Run `./install_bin_deps.sh` which will install `protoc-gen-gofast`, `protoc-gen-grpc`, and `protoc-gen-grpc-gateway` from vendor.


## Concepts

### Topics

Topics are topics. Each topic has its own directory, set of logfiles and indices, and its own
monotonically-increasing message offset. Each message publish writes to an append-only logfile
segment.

Topics are comprised of multiple segments. Segments are contiguous sections of logfile which are
created as each segment becomes too old or too big. slogd attempts to retain data no longer than
the topic is configured for. Segment age (for purposes of removal) is determined by the timestamp
of the newest message, whereas segment creation time is evaluated on the timestamp of the oldest 
message. Proactive segment rolling is based on segment creation time.

By default, segments are created every 6 hours or 16MiB. Each segment has two accompanying indices,
an index on offset and an index on publish timestamp. These allow for efficiently seeking through
the log.

### Durability

Slogd provides some durability guarantees. Upon receipt of an successful AppendLogs response, the data
in the request has been flushed to disk. However, it is possible that a large batch of messages may
trigger an early flush of data to disk. If slogd were to exit before a final flush, it may lead to some
messages being persisted and some lost messages, though should never result in corrupt segments. In that
case, the client would not have received a successful response and would retry the request.

Slogd may opt to implement transactional semantics in the future (prepare, append, {commit/abort}) to provide
stronger guarantees wherein an entire batch of messages will be committed to the log or not at all.

### Messages

Individual messages (slogd.LogEntry) have 5 different fields:

1) Offset. This is assigned by slogd at publish time, a monotonically increasing 64-bit integer.
This represents the order the message has in the topic. Offset 0 is the first message, offset 1 the
second and so on.

2) Timestamp. This is assigned by slogd at publish time, also monotonically increasing. That is,
m[n].timestamp < m[n+1].timestamp for all messages in the log. TODO: semantics around same-time delivery.

3) Payload. This is actually 2 fields: `protobuf` and `raw_bytes`. Only one may be set at any
given time (proto3 oneof). `protobuf` is a `google.protobuf.Any` type, whereas `raw_bytes` is,
well, raw bytes.

4) Annotations. This is a map of string to string KV pairs. These can be used to have slogd filter
messages at retrieval time. For instance, `userid -> 5` could be matched using an expression `k:userid v:5`,
but expressions may be arbitrarily complex. Individual clauses are Golang regexp and can be combined together, e.x.
`(k:userid v:vip\-[0-9]+) OR (k:important)`. 

### Message types

Messages can either be raw bytes, or a google.protobuf.Any type. An advantage of the Any type is
that it encodes a type URL with each message, allowing different kinds of messages to be safely
stored and read from the given log.

### Message retention

Like Kafka, slogd defaults to a 7 day retention period on log segments. This retention period is a
lower bound; a low-traffic segment may take longer to be purged. As segment age is determined by
last modification and not initial creation, data will last at least the retention period, but may
last longer. If segments are created every 6 hours, topic retention is 7 days, and the segment reaper
runs every 5 minutes, the oldest data be at worst 7 days, 6 hours, and 5 minutes old.

By tuning the max_segment_age, you can adjust the upper bound on data age.

## RPC methods

### Log management

### Topic management

### Stream semantics

Unlike Kafka, slogd has two modes of operation: poll-based and stream-based. Poll-based operation 
allows a client to ask for up to N messages starting at some offset X. This is how Kafka works. 
slogd also provides a stream-based API in which you can ask for messages starting at some offset X
(or latest) and slogd will maintain an open connection with your client, pushing messages as they
are published to the topic.

#### Internal: cursors

Each stream consumer (an RPC or HTTP consumer using the StreamLogs API) maintains an internal cursor 
state with slogd. This cursor allows for stateful iteration and low-latency pubsub semantics.

Each cursor maintains a Go channel serving as an unread message notifier. A task listens for messages on
this channel. A write to the log triggers a non-blocking write to this channel. Upon receipt of message,
the task submits a log read request starting at the offset last seen by the cursor. New messages are 
published to the consumer, new pages being requested from the underlying log until no more messages are 
returned, at which point the unread notification is checked again.

This mechanism is also used for catch-up, wherein a stream consumer can begin at an older log offset
and consume until reaching the end of the log.

## TODO

- [ ] Cursors should be able to page reads without invoking index lookup
- [ ] Consumer CLI tool
- [ ] Configuration file
- [ ] Topic configuration
- [ ] Implement latest/earliest lookup
- [ ] Implement timestamp-based indexes and lookup
- [ ] re2-based annotation filters