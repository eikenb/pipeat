package pipeat

// Attempts to provide similar funcationality to io.Pipe except supporting
// ReaderAt and WriterAt. It uses a temp file as a buffer in order to implement
// the interfaces as well as allow for asyncronous writes.

import (
	"errors"
	"fmt"
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
	rerr      chan error
	werr      chan error
}

func newPipeFile() (*pipeFile, error) {
	file, err := ioutil.TempFile("", "test")
	if err != nil {
		return nil, err
	}
	os.Remove(file.Name())
	f := &pipeFile{File: file,
		rerr: make(chan error),
		werr: make(chan error)}
	f.L = f.fileLock.RLocker() // Cond locker
	return f, nil
}

func (f *pipeFile) readable(start, end int64) bool {
	f.dataLock.RLock()
	defer f.dataLock.RUnlock()
	return (start+end < f.endln)
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
// each other. Due to the asyncronous nature, Close needs to be called on the
// writer first, then the reader. If you call Close on the reader first it will
// block until the Writer's Close is called.
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
		if !r.f.readable(off, int64(len(p))) {
			select {
			case err := <-r.f.rerr:
				if err == nil {
					err = ErrClosedPipe
				}
				return 0, err
			case <-r.f.werr:
				// writer closed
			default:
				r.f.Wait()
				continue
			}
		}
		n, err := r.f.File.ReadAt(p, off)
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

// Close will block until writer closes, then it will Close the temp file.
func (r *PipeReaderAt) Close() error {
	r.f.fileLock.Lock()
	defer r.f.fileLock.Unlock()
	close(r.f.rerr)
	return r.f.File.Close()
}

// WriteAt implements the standard WriterAt interface. It will write to the
// temp file without blocking. You can call it from multiple threads.
func (w *PipeWriterAt) WriteAt(p []byte, off int64) (int, error) {
	trace("writeat: ", string(p), off)
	defer trace("wrote: ", string(p), off)
	w.f.fileLock.RLock()
	defer w.f.fileLock.RUnlock()
	n, err := w.f.File.WriteAt(p, off)

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
		sort.Sort(w.f.ahead)
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
	close(w.f.werr)
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
