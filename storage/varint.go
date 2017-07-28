// Protocol Buffers for Go with Gadgets
//
// Copyright (c) 2013, The GoGo Authors. All rights reserved.
// http://github.com/gogo/protobuf
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package storage

import (
	"bufio"
	"encoding/binary"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"io"
)

var (
	errSmallBuffer = errors.New("Buffer Too Small")
	errLargeValue  = errors.New("Value is Larger than 64 bits")
)

type Reader interface {
	ReadMsg(msg proto.Message) (uint64, error)
}

type ReadCloser interface {
	Reader
	io.Closer
}

type Writer interface {
	WriteMsg(proto.Message) (int, error)
}

type WriteCloser interface {
	Writer
	io.Closer
}

func NewDelimitedWriter(w io.Writer) WriteCloser {
	return &varintWriter{w, make([]byte, 10), nil}
}

type varintWriter struct {
	w      io.Writer
	lenBuf []byte
	buffer []byte
}

func (this *varintWriter) WriteMsg(msg proto.Message) (bytesWritten int, err error) {
	var data []byte
	data, err = proto.Marshal(msg)
	if err != nil {
		return 0, errors.Wrap(err, "Error marshalling proto")
	}

	length := uint64(len(data))
	n := binary.PutUvarint(this.lenBuf, length)
	_, err = this.w.Write(this.lenBuf[:n])
	if err != nil {
		return 0, errors.Wrap(err, "Error writing proto to buffer")
	}
	_, err = this.w.Write(data)
	return n + len(data), errors.Wrap(err, "Error writing to storage")
}

func (this *varintWriter) Close() error {
	if closer, ok := this.w.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func NewDelimitedReader(r io.Reader, maxSize int) ReadCloser {
	var closer io.Closer
	if c, ok := r.(io.Closer); ok {
		closer = c
	}
	return &varintReader{bufio.NewReader(r), make([]byte, 10), maxSize, closer}
}

type varintReader struct {
	r       *bufio.Reader
	buf     []byte
	maxSize int
	closer  io.Closer
}

func (this *varintReader) ReadMsg(msg proto.Message) (bytesRead uint64, err error) {
	length64, err := binary.ReadUvarint(this.r)
	if err != nil {
		return 0, err // Must return io.EOF
	}

	// XXX hack :(
	bytesRead += uint64(binary.PutUvarint(this.buf, length64))

	length := int(length64)
	if length < 0 || length > this.maxSize {
		return bytesRead, io.ErrShortBuffer
	}
	if len(this.buf) < length {
		this.buf = make([]byte, length)
	}
	buf := this.buf[:length]
	n, err := io.ReadFull(this.r, buf)
	bytesRead += uint64(n)
	if err != nil {
		return bytesRead, err // Must return io.EOF
	}
	return bytesRead, proto.Unmarshal(buf, msg)
}

func (this *varintReader) Close() error {
	if this.closer != nil {
		return this.closer.Close()
	}
	return nil
}
