# Structured Log Daemon (slogd)

slogd is a TCP-accessible, low-resource, Kafka-like log daemon built with protocol buffers
as a first-class citizen.

slogd aims to be a Kafka-like tool for the enthusiast developer, without the investment of
running Zookeeper and Kafka on a single node. While the JVM can be tuned for lower resources,
it will always be handily beat by a well-written Go program. slogd runs in approximately 10kb
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

## Concepts

### Topics

Topics are topics. Each topic has its own directory, set of logfiles and indices, and its own
monotonically-increasing message offset. Each message publish writes to an append-only logfile
segment.

Topics are comprised of multiple segments. Segments are contiguous sections of logfile which are
created as each segment becomes too old or too big. slogd attempts to retain data no longer than
the topic is configured for. Segment age is determined via file metadata, touching the file mtime
of a segment will "reset the clock" on a given segment.

By default, segments are created every 6 hours or 100mb. Each segment has two accompanying indices,
an index on offset and an index on publish timestamp. These allow for efficiently seeking through
the log.

### Message types

Messages can either be raw bytes, or a google.protobuf.Any type. An advantage of the Any type is
that it encodes a type URL with each message, allowing different kinds of messages to be safely
stored and read from the given log.

### Message retention

Like Kafka, slogd defaults to a 7 day retention period on log segments. This retention period is a
lower bound; a low-traffic segment may take longer to be purged. As segment age is determined by
last modification and not initial creation, data will last at least the retention period, but may
last longer. If segments are created every 6 hours, the oldest data will last at most 7 days, 6 hours
before being purged.

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
- [ ] Timed log roller process
- [ ] Segment reaper
- [ ] Consumer CLI tool
- [ ] Configuration file
- [ ] Topic configuration
- [ ] Implement latest/earliest lookup
- [ ] Implement timestamp-based indexes and lookup
- [ ] re2-based annotation filters