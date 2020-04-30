package pipeat

// Author: John Eikenberry <jae@zhar.net>
// License: CC0 <http://creativecommons.org/publicdomain/zero/1.0/>

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var paragraph = "In programming, concurrency is the composition of independently executing processes, while parallelism is the simultaneous execution of (possibly related) computations. Concurrency is about dealing with lots of things at once. Parallelism is about doing lots of things at once."

type chunk struct {
	offset int64
	word   []byte
}

func chunkify(s string) []chunk {
	ss := strings.Split(s, " ")
	chunks := make([]chunk, len(ss))
	offset := 0
	for i, s := range ss {
		if i+1 != len(ss) {
			s = s + " "
		}
		chunks[i] = chunk{int64(offset), []byte(s)}
		offset += len(s)
	}
	return chunks
}

func segmentify(chunks []chunk, num int) [][]chunk {
	segments := make([][]chunk, num)
	segSize := (len(chunks) / num) + 1
	var start, end int
	for i := 0; i < num; i++ {
		start, end = end, end+segSize
		if end <= len(chunks) {
			segments[i] = chunks[start:end]
		} else {
			segments[i] = chunks[start:]
		}
	}
	return segments
}

func reverse(t []chunk) []chunk {
	for l, r := 0, len(t)-1; l < r; l, r = l+1, r-1 {
		t[l], t[r] = t[r], t[l]
	}
	return t
}

type readers interface {
	io.Reader
	io.ReaderAt
}

type reader struct {
	r           readers
	buf         []byte
	*sync.Mutex // to protect buf
}

func newReader(r readers) *reader {
	return &reader{
		r:     r,
		Mutex: &sync.Mutex{},
	}
}

func (r *reader) writeat(b []byte, off int) {
	r.Lock()
	defer r.Unlock()
	// fmt.Println("reader writeat", len(b), off, string(b))
	if off+len(b) < len(r.buf) {
		copy(r.buf[off:], b)
	} else if off == len(r.buf) {
		r.buf = append(r.buf, b...)
	} else {
		ndata := make([]byte, off+len(b))
		copy(ndata, r.buf)
		copy(ndata[off:], b)
		r.buf = ndata
	}
}

func (r *reader) String() string {
	r.Lock()
	defer r.Unlock()
	return string(r.buf)
}

func sectionReader(r *reader, start, end int64) *reader {
	section := io.NewSectionReader(r.r, start, end)
	return newReader(section)
}

type readat func([]byte, int64) (int, error)
type write func([]byte, int)

func basicReader(r readat, w write) {
	offset := 0
	size := 10
	for {
		b := make([]byte, size)
		n, err := r(b, int64(offset))
		w(b[:n], offset)
		if err == io.EOF {
			return
		}
		if err != nil {
			panic(err)
		}
		offset += n
	}
}

func simpleReader(t *testing.T, r *reader) {
	defer r.r.(*PipeReaderAt).Close()
	basicReader(r.r.ReadAt, r.writeat)
}

func ioReader(t *testing.T, r *reader) {
	defer r.r.(*PipeReaderAt).Close()
	read := func(b []byte, _ int64) (int, error) {
		return r.r.Read(b)
	}
	basicReader(read, r.writeat)
}

func ccReader(t *testing.T, r *reader, workers int) {
	defer r.r.(*PipeReaderAt).Close()
	segSize := int64((len(paragraph) / workers) + 1)
	var sections = make([]*reader, workers)
	var start, end int64
	for i := 0; i < workers; i++ {
		start, end = end, end+segSize
		sections[i] = sectionReader(r, start, segSize)
	}
	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			s := sections[i]
			basicReader(s.r.ReadAt, s.writeat)
			wg.Done()
		}(i)
	}
	wg.Wait()
	buf := bytes.NewBuffer(nil)
	for i := 0; i < workers; i++ {
		buf.Write(sections[i].buf)
	}
	r.writeat(buf.Bytes(), 0)
}

func simpleWriter(t *testing.T, w *PipeWriterAt) {
	for _, t := range chunkify(paragraph) {
		w.WriteAt(t.word, t.offset)
	}
	w.Close()
}

func ioWriter(t *testing.T, w *PipeWriterAt) {
	for _, t := range chunkify(paragraph) {
		w.Write(t.word)
	}
	w.Close()
}

func reverseWriter(t *testing.T, w *PipeWriterAt) {
	chunks := reverse(chunkify(paragraph))
	for _, t := range chunks {
		w.WriteAt(t.word, t.offset)
	}
	w.Close()
}

func randomWriter(t *testing.T, w *PipeWriterAt) {
	chunks := chunkify(paragraph)
	picks := rand.Perm(len(chunks))
	for _, i := range picks {
		t := chunks[i]
		w.WriteAt(t.word, t.offset)
	}
	w.Close()
}

func ccWriter(t *testing.T, w *PipeWriterAt, workers int) {
	chunks := chunkify(paragraph)
	segments := segmentify(chunks, workers)
	resultsWg := &sync.WaitGroup{}
	resultsWg.Add(workers)
	for i := 0; i < len(segments); i++ {
		go func(j int, segment []chunk) {
			for _, t := range segment {
				w.WriteAt(t.word, t.offset)
			}
			resultsWg.Done()
		}(i, segments[i])
	}
	resultsWg.Wait()
	w.Close()
}

func TestCcWrite(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	workers := 4

	reader := newReader(r)
	go simpleReader(t, reader)
	go ccWriter(t, w, workers)
	w.WaitForReader()
	assert.Equal(t, reader.String(), paragraph)
	assert.Equal(t, r.GetReadedBytes(), int64(len(paragraph)))
	assert.Equal(t, w.GetWrittenBytes(), int64(len(paragraph)))
}

func TestCcRead(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	workers := 4

	reader := newReader(r)
	go ccReader(t, reader, workers)
	go simpleWriter(t, w)
	w.WaitForReader()
	assert.Equal(t, reader.String(), paragraph)
	assert.Equal(t, r.GetReadedBytes(), int64(len(paragraph)))
	assert.Equal(t, w.GetWrittenBytes(), int64(len(paragraph)))
}

func TestCcRWrite(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	workers := 4

	reader := newReader(r)
	go ccReader(t, reader, workers)
	go ccWriter(t, w, workers)
	w.WaitForReader()
	assert.Equal(t, reader.String(), paragraph)
	assert.Equal(t, r.GetReadedBytes(), int64(len(paragraph)))
	assert.Equal(t, w.GetWrittenBytes(), int64(len(paragraph)))
}

func TestSimpleWrite(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	reader := newReader(r)
	go simpleReader(t, reader)
	go simpleWriter(t, w)
	w.WaitForReader()
	trace(reader.String())
	assert.Equal(t, reader.String(), paragraph)
	assert.Equal(t, r.GetReadedBytes(), int64(len(paragraph)))
	assert.Equal(t, w.GetWrittenBytes(), int64(len(paragraph)))
}

func TestWaitOnRead(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	reader := newReader(r)
	go func() {
		time.Sleep(time.Millisecond)
		simpleReader(t, reader)
	}()
	simpleWriter(t, w)
	w.WaitForReader()
	assert.Equal(t, reader.String(), paragraph)
	assert.Equal(t, r.GetReadedBytes(), int64(len(paragraph)))
	assert.Equal(t, w.GetWrittenBytes(), int64(len(paragraph)))
}

func TestReverseWrite(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	reader := newReader(r)
	go simpleReader(t, reader)
	go reverseWriter(t, w)
	w.WaitForReader()
	trace(reader.String())
	assert.Equal(t, reader.String(), paragraph)
	assert.Equal(t, r.GetReadedBytes(), int64(len(paragraph)))
	assert.Equal(t, w.GetWrittenBytes(), int64(len(paragraph)))
}

func TestRandomWrite(t *testing.T) {
	rand.Seed(17)
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	reader := newReader(r)
	go simpleReader(t, reader)
	go randomWriter(t, w)
	w.WaitForReader()
	trace(reader.String(), len(reader.String()))
	assert.Equal(t, reader.String(), paragraph)
	assert.Equal(t, r.GetReadedBytes(), int64(len(paragraph)))
	assert.Equal(t, w.GetWrittenBytes(), int64(len(paragraph)))
}

// io.Read paired with concurrent writer
func TestIoRead(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	workers := 4

	reader := newReader(r)
	go ioReader(t, reader)
	go ccWriter(t, w, workers)
	w.WaitForReader()
	assert.Equal(t, reader.String(), paragraph)
	assert.Equal(t, r.GetReadedBytes(), int64(len(paragraph)))
	assert.Equal(t, w.GetWrittenBytes(), int64(len(paragraph)))
}

// io.Writer paired wit concurrent reader
func TestIoWrite(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	workers := 4

	reader := newReader(r)
	go ccReader(t, reader, workers)
	go ioWriter(t, w)
	w.WaitForReader()
	assert.Equal(t, reader.String(), paragraph)
	assert.Equal(t, r.GetReadedBytes(), int64(len(paragraph)))
	assert.Equal(t, w.GetWrittenBytes(), int64(len(paragraph)))
}

// test close
func TestReadClose(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	r.Close()
	b := make([]byte, 1)
	n, err := r.ReadAt(b, 10)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
	b = []byte("hi")
	n, err = w.WriteAt(b, 10)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}

func TestReadCloseWithError(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	errTest := errors.New("test error")
	r.CloseWithError(errTest)
	b := make([]byte, 1)
	n, err := r.ReadAt(b, 10)
	assert.Equal(t, 0, n)
	assert.Equal(t, errTest, err)
	b = []byte("hi")
	n, err = w.WriteAt(b, 10)
	assert.Equal(t, 0, n)
	assert.Equal(t, errTest, err)
}

func TestWriteClose(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	go func() {
		w.Close()
	}()
	<-w.f.eow // used to detect that close is done except block on reader
	b := []byte("hi")
	n, err := w.WriteAt(b, 10)
	assert.Equal(t, 0, n, "shouldn't have been able to write")
	assert.Equal(t, io.EOF, err)
	b = make([]byte, 1)
	n, err = r.ReadAt(b, 10)
	assert.Equal(t, 0, n, "shouldn't have been able to read")
	assert.Equal(t, io.EOF, err)
}

func TestWriteCloseWithError(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	errTest := errors.New("test error")
	go func() {
		w.CloseWithError(errTest)
	}()
	<-w.f.eow // used to detect that close is done except block on reader
	b := []byte("hi")
	n, err := w.WriteAt(b, 10)
	assert.Equal(t, 0, n, "shouldn't have been able to write")
	assert.Equal(t, errTest, err)
	b = make([]byte, 1)
	n, err = r.ReadAt(b, 10)
	assert.Equal(t, 0, n, "shouldn't have been able to read")
	assert.Equal(t, errTest, err)
	r.Close()
}

// same as above, but tests AsyncWriterPipe()
func TestAsyncWriteCloseWithError(t *testing.T) {
	r, w, err := AsyncWriterPipe()
	if err != nil {
		panic(err)
	}
	errTest := errors.New("test error")
	w.CloseWithError(errTest)
	b := []byte("hi")
	n, err := w.WriteAt(b, 10)
	assert.Equal(t, 0, n)
	assert.Equal(t, errTest, err)
	b = make([]byte, 1)
	n, err = r.ReadAt(b, 10)
	assert.Equal(t, 0, n)
	assert.Equal(t, errTest, err)
}

func TestPipeInDir(t *testing.T) {
	r, w, err := PipeInDir("")
	if err != nil {
		panic(err)
	}
	err = r.Close()
	if err != nil {
		panic(err)
	}
	err = w.Close()
	if err != nil {
		panic(err)
	}
	_, _, err = PipeInDir("invalid dir")
	if err == nil {
		panic("pipe in dir must file, the requested directory does not exists")
	}
}

func TestAsyncPipeInDir(t *testing.T) {
	r, w, err := AsyncWriterPipeInDir("")
	if err != nil {
		panic(err)
	}
	err = w.Close()
	if err != nil {
		panic(err)
	}
	err = r.Close()
	if err != nil {
		panic(err)
	}
	_, _, err = AsyncWriterPipeInDir("invalid dir")
	if err == nil {
		panic("async writer pipe in dir must file, the requested directory does not exists")
	}
}

func TestUnlockWriteOnReaderClose(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	// the first write will succeed, the second one will remain blocked
	// waiting for the reader, when the reader is closed waitForWritable
	// returns and WriteAt can finish
	errTest := errors.New("test error")
	b := []byte("hi")
	n, err := w.WriteAt(b, 0)
	assert.Equal(t, len(b), n)
	assert.Equal(t, nil, err)
	go func() {
		r.CloseWithError(errTest)
	}()
	// this write will be unlocked when close ends
	n, err = w.WriteAt(b, 5)
	assert.Equal(t, 0, n)
	assert.Equal(t, errTest, err)
	w.Close()
}

func TestUnlockWriteOnWriterClose(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	// the first write will succeed, the second one will remain blocked
	// waiting for the reader, when the writer is closed waitForWritable
	// returns and WriteAt can finish
	errTest := errors.New("test error")
	b := []byte("hi")
	n, err := w.WriteAt(b, 0)
	assert.Equal(t, len(b), n)
	assert.Equal(t, nil, err)
	go func() {
		w.CloseWithError(errTest)
	}()
	// this write will be unlocked when close ends
	n, err = w.WriteAt(b, 5)
	assert.Equal(t, 0, n)
	assert.Equal(t, errTest, err)
	r.Close()
}
