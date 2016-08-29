package pipeat

// Attempts to provide similar funcationality to io.Pipe except supporting
// ReaderAt and WriterAt. It uses a temp file as a buffer in order to implement
// the interfaces as well as allow for asyncronous writes.

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
)

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
	sync.Mutex // serialize access to meta data (below)
	sync.Cond  // used with Mutex to signal readers from writers
	endln      int64
	ahead      spans
	eof        chan struct{} // closed when all writes complete
	readeroff  int64         // offset where Read() last read
}

func newPipeFile() (*pipeFile, error) {
	file, err := ioutil.TempFile("", "test")
	if err != nil {
		return nil, err
	}
	os.Remove(file.Name())
	b := &pipeFile{File: file, eof: make(chan struct{})}
	b.L = b // Cond locker
	return b, nil
}

func (b *pipeFile) readable(start, end int64) bool {
	return (start+end < b.endln)
}

// io.WriterAt side of pipe.
type PipeWriterAt struct {
	b *pipeFile
}

// io.ReaderAt side of pipe.
type PipeReaderAt struct {
	b *pipeFile
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
	r.b.Lock()
	for {
		if !r.b.readable(off, int64(len(p))) {
			select {
			case <-r.b.eof:
				trace("EOF")
			default:
				r.b.Wait()
				continue
			}
		}
		r.b.Unlock()
		return r.b.File.ReadAt(p, off)
	}
}

// It can also function as a io.Reader
func (r *PipeReaderAt) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.b.readeroff)
	if err == nil {
		r.b.Lock()
		defer r.b.Unlock()
		r.b.readeroff = r.b.readeroff + int64(n)
	}
	return n, err
}

// Close will block until writer closes, then it will Close the temp file.
func (r *PipeReaderAt) Close() error {
	select {
	case <-r.b.eof:
		r.b.Lock()
		defer r.b.Unlock()
		return r.b.File.Close()
	}
}

// WriteAt implements the standard WriterAt interface. It will write to the
// temp file without blocking. You can call it from multiple threads.
func (w *PipeWriterAt) WriteAt(p []byte, off int64) (int, error) {
	trace("writeat: ", string(p), off)
	defer trace("wrote: ", string(p), off)
	n, err := w.b.File.WriteAt(p, off)
	w.b.Lock()
	defer w.b.Unlock()
	if off == w.b.endln {
		w.b.endln = w.b.endln + int64(n)
		newtip := 0
		for i, s := range w.b.ahead {
			if s.start == w.b.endln {
				w.b.endln = s.end
				newtip = i + 1
			}
		}
		if newtip > 0 { // clean up ahead queue
			w.b.ahead = append(w.b.ahead[:0], w.b.ahead[newtip:]...)
		}
		w.b.Broadcast()
	} else {
		// XXX should be limit on ahead's length
		w.b.ahead = append(w.b.ahead, span{off, off + int64(n)})
		sort.Sort(w.b.ahead)
	}
	// trace(w.b.ahead)
	return n, err
}

// Write provides a standard io.Writer interface.
func (w *PipeWriterAt) Write(p []byte) (int, error) {
	n, err := w.b.Write(p)
	if err == nil {
		w.b.Lock()
		defer w.b.Unlock()
		w.b.endln += int64(n)
		w.b.Broadcast()
	}
	return n, err
}

// Close on the writer will let the reader know that writing is complete. Once
// the reader catches up it will continue to return 0 bytes and an EOF error.
func (w *PipeWriterAt) Close() error {
	w.b.Lock()
	defer w.b.Unlock()
	close(w.b.eof)
	w.b.Broadcast()
	return nil
}

// debugging stuff
const watch = false

func trace(p ...interface{}) {
	if watch {
		fmt.Println(p...)
	}
}
