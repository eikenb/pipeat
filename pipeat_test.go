package pipeat

import (
	"bytes"
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
	seg_size := (len(chunks) / num) + 1
	var start, end int
	for i := 0; i < num; i++ {
		start, end = end, end+seg_size
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

func slowdown(i int) time.Duration {
	return time.Duration(i * 13)
}

type readers interface {
	io.Reader
	io.ReaderAt
}

type reader struct {
	r           readers
	wg          *sync.WaitGroup
	delay       int
	dest        chan chunk
	buf         *bytes.Buffer
	*sync.Mutex // to protect buf
}

func newReader(r readers, wg *sync.WaitGroup, delay int) *reader {
	return &reader{
		r: r, wg: wg, delay: delay,
		dest:  make(chan chunk, 1000), // big enough to hold all words
		Mutex: &sync.Mutex{},
	}
}

func sectionReader(r *reader, wg *sync.WaitGroup, start, end int64) *reader {
	section := io.NewSectionReader(r.r, start, end)
	return newReader(section, wg, r.delay)
}

func (r *reader) buffer() {
	r.Lock()
	defer r.Unlock()
	if r.buf != nil {
		return
	}
	var size int
	parts := []chunk{}
	close(r.dest)
	for t := range r.dest {
		parts = append(parts, t)
		size += len(t.word)
	}
	trace("parts length:", len(parts))
	result := make([]byte, size)
	for _, t := range parts {
		trace("--", t.offset, len(t.word), string(t.word))
		start, end := t.offset, len(t.word)+int(t.offset)
		copy(result[start:end], t.word)
	}
	r.buf = bytes.NewBuffer(result)
}

func (r *reader) String() string {
	r.buffer()
	return r.buf.String()
}

func simpleReader(t *testing.T, r *reader) {
	defer r.wg.Done()
	offset := 0
	size := 10
	for {
		b := make([]byte, size)
		n, err := r.r.ReadAt(b, int64(offset))
		if err != nil && err != io.EOF {
			panic(err)
		}
		r.dest <- chunk{offset: int64(offset), word: b[:n]}
		if n == 0 || err == io.EOF {
			return
		}
		offset += n
		time.Sleep(time.Millisecond * time.Duration(r.delay))
	}
}

func ioReader(t *testing.T, r *reader) {
	defer r.wg.Done()
	offset := 0
	size := 10
	for {
		b := make([]byte, size)
		n, err := r.r.Read(b)
		if err != nil && err != io.EOF {
			panic(err)
		}
		r.dest <- chunk{offset: int64(offset), word: b[:n]}
		if n == 0 || err == io.EOF {
			return
		}
		offset += n
		time.Sleep(time.Millisecond * time.Duration(r.delay))
	}
}

func ccReader(t *testing.T, r *reader, workers int) {
	seg_size := int64((len(paragraph) / workers) + 1)
	var sections = make([]*reader, workers)
	var start, end int64
	results_wg := &sync.WaitGroup{}
	results_wg.Add(workers)
	for i := 0; i < workers; i++ {
		start, end = end, end+seg_size
		sections[i] = sectionReader(r, results_wg, start, seg_size)
	}
	for i := 0; i < workers; i++ {
		go simpleReader(t, sections[i])
	}
	results_wg.Wait()
	r.buf = bytes.NewBuffer(nil)
	for i := 0; i < workers; i++ {
		sections[i].buffer()
		r.buf.Write(sections[i].buf.Bytes())
		r.wg.Done()
	}
}

func simpleWriter(t *testing.T, w *PipeWriterAt, delay int) {
	for _, t := range chunkify(paragraph) {
		time.Sleep(time.Millisecond * time.Duration(delay))
		w.WriteAt(t.word, t.offset)
	}
	w.Close()
}

func ioWriter(t *testing.T, w *PipeWriterAt, delay int) {
	for _, t := range chunkify(paragraph) {
		time.Sleep(time.Millisecond * time.Duration(delay))
		w.Write(t.word)
	}
	w.Close()
}

func reverseWriter(t *testing.T, w *PipeWriterAt, delay int) {
	chunks := reverse(chunkify(paragraph))
	for _, t := range chunks {
		time.Sleep(time.Millisecond * time.Duration(delay))
		w.WriteAt(t.word, t.offset)
	}
	w.Close()
}

func randomWriter(t *testing.T, w *PipeWriterAt, delay int) {
	chunks := chunkify(paragraph)
	picks := rand.Perm(len(chunks))
	for _, i := range picks {
		t := chunks[i]
		time.Sleep(time.Millisecond * time.Duration(delay))
		w.WriteAt(t.word, t.offset)
	}
	w.Close()
}

func ccWriter(t *testing.T, w *PipeWriterAt, delay, workers int) {
	chunks := chunkify(paragraph)
	segments := segmentify(chunks, workers)
	results_wg := &sync.WaitGroup{}
	results_wg.Add(workers)
	for i := 0; i < len(segments); i++ {
		go func(j int, segment []chunk) {
			for _, t := range segment {
				time.Sleep(time.Millisecond * time.Duration(delay))
				w.WriteAt(t.word, t.offset)
			}
			results_wg.Done()
		}(i, segments[i])
	}
	results_wg.Wait()
	w.Close()
}

func TestCcWrite(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	workers := 4

	wg := &sync.WaitGroup{}
	wg.Add(1)
	reader := newReader(r, wg, 0)
	go simpleReader(t, reader)
	go ccWriter(t, w, 0, workers)
	wg.Wait()
	r.Close()
	assert.Equal(t, reader.String(), paragraph)
}

func TestCcRead(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	workers := 4

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	reader := newReader(r, wg, 0)
	go ccReader(t, reader, workers)
	go simpleWriter(t, w, 1)
	wg.Wait()
	r.Close()
	assert.Equal(t, reader.String(), paragraph)
}

func TestCcRWrite(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	workers := 4

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	reader := newReader(r, wg, 0)
	go ccReader(t, reader, workers)
	go ccWriter(t, w, 0, workers)
	wg.Wait()
	r.Close()
	assert.Equal(t, reader.String(), paragraph)
}

func TestSimpleWrite(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	reader := newReader(r, wg, 0)
	go simpleReader(t, reader)
	// Writer
	go simpleWriter(t, w, 0)
	wg.Wait()
	r.Close()
	trace(reader.String())
	assert.Equal(t, reader.String(), paragraph)
}

func TestReverseWrite(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	reader := newReader(r, wg, 0)
	go simpleReader(t, reader)
	go reverseWriter(t, w, 0)
	wg.Wait()
	r.Close()
	trace(reader.String())
	assert.Equal(t, reader.String(), paragraph)
}

func TestRandomWrite(t *testing.T) {
	rand.Seed(17)
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	reader := newReader(r, wg, 0)
	go simpleReader(t, reader)
	go randomWriter(t, w, 1)
	wg.Wait()
	r.Close()
	trace(reader.String(), len(reader.String()))
	assert.Equal(t, reader.String(), paragraph)
}

// io.Read paired with concurrent writer
func TestIoRead(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	workers := 4

	wg := &sync.WaitGroup{}
	wg.Add(1)
	reader := newReader(r, wg, 0)
	go ioReader(t, reader)
	go ccWriter(t, w, 0, workers)
	wg.Wait()
	r.Close()
	assert.Equal(t, reader.String(), paragraph)
}

// io.Writer paired wit concurrent reader
func TestIoWrite(t *testing.T) {
	r, w, err := Pipe()
	if err != nil {
		panic(err)
	}
	workers := 4

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	reader := newReader(r, wg, 0)
	go ccReader(t, reader, workers)
	go ioWriter(t, w, 0)
	wg.Wait()
	r.Close()
	assert.Equal(t, reader.String(), paragraph)
}
