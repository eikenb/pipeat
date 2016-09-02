package pipeat

// Attempts to provide similar funcationality to io.Pipe except supporting
// ReaderAt and WriterAt. It uses a temp file as a buffer in order to implement
// the interfaces as well as allow for asyncronous writes.

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"sync"
)

var ErrClosedPipe = errors.New("io: read/write on closed pipe")

// Used to track write ahead areas of a file. That is, areas where there is a
// gap in the file data earlier in the file. Possible with concurrent writes.
type span struct {
	start, end int64
}

type spans []span

func (c spans) Len() int           { return len(c) }
func (c spans) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c spans) Less(i, j int) bool { return c[i].start < c[j].start }

// The wrapper around the underlying temp file.
type pipeFile struct {
	*os.File
	fileLock  sync.RWMutex
	sync.Cond              // used to signal readers from writers
	dataLock  sync.RWMutex // serialize access to meta data (below)
	endln     int64
	ahead     spans
	readeroff int64 // offset where Read() last read
	rerr      error
	werr      error
	eof       chan struct{}
}

func newPipeFile() (*pipeFile, error) {
	file, err := ioutil.TempFile("", "test")
	if err != nil {
		return nil, err
	}
	os.Remove(file.Name())
	f := &pipeFile{File: file, eof: make(chan struct{})}
	f.L = f.fileLock.RLocker() // Cond locker
	return f, nil
}

func (f *pipeFile) readable(start, end int64) bool {
	f.dataLock.RLock()
	defer f.dataLock.RUnlock()
	return (start+end < f.endln)
}

func (f *pipeFile) readerror() error {
	f.dataLock.RLock()
	defer f.dataLock.RUnlock()
	return f.rerr
}

func (f *pipeFile) setReaderror(err error) {
	f.dataLock.Lock()
	defer f.dataLock.Unlock()
	f.rerr = err
}

func (f *pipeFile) writeerror() error {
	f.dataLock.RLock()
	defer f.dataLock.RUnlock()
	return f.werr
}

func (f *pipeFile) setWriteerror(err error) {
	f.dataLock.Lock()
	defer f.dataLock.Unlock()
	f.werr = err
}

// io.WriterAt side of pipe.
type PipeWriterAt struct {
	f *pipeFile
}

// io.ReaderAt side of pipe.
type PipeReaderAt struct {
	f *pipeFile
}

// Pipe creates an asynchronous file based pipe. It can be used to connect code
// expecting an io.ReaderAt with code expecting an io.WriterAt. Writes all go
// to an unlinked temporary file, reads start up as the file gets written up to
// their area. It is safe to call multiple ReadAt and WriteAt in parallel with
// each other.
func Pipe() (*PipeReaderAt, *PipeWriterAt, error) {
	fp, err := newPipeFile()
	if err != nil {
		return nil, nil, err
	}
	return &PipeReaderAt{fp}, &PipeWriterAt{fp}, nil
}

// ReadAt implements the standard ReaderAt interface. It blocks if it gets
// ahead of the writer. You can call it from multiple threads.
func (r *PipeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	trace("readat", off)
	defer trace("successfully read:", off, string(p))

	r.f.fileLock.RLock()
	defer r.f.fileLock.RUnlock()
	for {
		if err := r.f.readerror(); err != nil {
			return 0, err
		}

		if !r.f.readable(off, int64(len(p))) {
			select {
			case <-r.f.eof:
				trace("eof")
			default:
				r.f.Wait()
				continue
			}
		}
		n, err := r.f.File.ReadAt(p, off)
		if err != nil {
			if werr := r.f.writeerror(); werr != nil {
				err = werr
			}
		}
		return n, err
	}
}

// It can also function as a io.Reader
func (r *PipeReaderAt) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.f.readeroff)
	if err == nil {
		r.f.dataLock.Lock()
		defer r.f.dataLock.Unlock()
		r.f.readeroff = r.f.readeroff + int64(n)
	}
	return n, err
}

// Close will Close the temp file and subsequent writes or reads will return an
// ErrClosePipe error.
func (r *PipeReaderAt) Close() error {
	return r.CloseWithError(nil)
}

// CloseWithError sets error and otherwise behaves like Close.
func (r *PipeReaderAt) CloseWithError(err error) error {
	if err == nil {
		err = ErrClosedPipe
	}
	r.f.setReaderror(err)
	r.f.fileLock.Lock()
	defer r.f.fileLock.Unlock()
	return r.f.File.Close()
}

// WriteAt implements the standard WriterAt interface. It will write to the
// temp file without blocking. You can call it from multiple threads.
func (w *PipeWriterAt) WriteAt(p []byte, off int64) (int, error) {
	trace("writeat: ", string(p), off)
	defer trace("wrote: ", string(p), off)

	w.f.fileLock.RLock()
	defer w.f.fileLock.RUnlock()
	if err := w.f.writeerror(); err != nil {
		return 0, err
	}
	n, err := w.f.File.WriteAt(p, off)
	if err != nil {
		if err := w.f.readerror(); err != nil {
			return 0, err
		}
		return 0, ErrClosedPipe
	}

	w.f.dataLock.Lock()
	defer w.f.dataLock.Unlock()

	if off == w.f.endln {
		w.f.endln = w.f.endln + int64(n)
		newtip := 0
		for i, s := range w.f.ahead {
			if s.start == w.f.endln {
				w.f.endln = s.end
				newtip = i + 1
			}
		}
		if newtip > 0 { // clean up ahead queue
			w.f.ahead = append(w.f.ahead[:0], w.f.ahead[newtip:]...)
		}
		w.f.Broadcast()
	} else {
		w.f.ahead = append(w.f.ahead, span{off, off + int64(n)})
		sort.Sort(w.f.ahead) // should already be sorted..
	}
	// trace(w.f.ahead)
	return n, err
}

// Write provides a standard io.Writer interface.
func (w *PipeWriterAt) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	if err == nil {
		w.f.dataLock.Lock()
		defer w.f.dataLock.Unlock()
		w.f.endln += int64(n)
		w.f.Broadcast()
	}
	return n, err
}

// Close on the writer will let the reader know that writing is complete. Once
// the reader catches up it will continue to return 0 bytes and an EOF error.
func (w *PipeWriterAt) Close() error {
	return w.CloseWithError(nil)
}

// CloseWithError sets the error and otherwise behaves like Close.
func (w *PipeWriterAt) CloseWithError(err error) error {
	if err == nil {
		err = io.EOF
	}
	w.f.setWriteerror(err)
	close(w.f.eof)
	w.f.Broadcast()
	return nil
}

// debugging stuff
const watch = false

func trace(p ...interface{}) {
	if watch {
		fmt.Println(p...)
	}
}
