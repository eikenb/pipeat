package pipeat

// Attempts to provide similar funcationality to io.Pipe except supporting
// ReaderAt and WriterAt. It uses a temp file as a buffer in order to implement
// the interfaces as well as allow for asyncronous writes.

// Author: John Eikenberry <jae@zhar.net>
// License: CC0 <http://creativecommons.org/publicdomain/zero/1.0/>

import (
	"fmt"
	"io"
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
	fileLock  sync.RWMutex
	rCond     sync.Cond    // used to signal readers from writers
	wCond     sync.Cond    // used to signal writers from readers in sync mode
	dataLock  sync.RWMutex // serialize access to meta data (below)
	syncMode  bool         // if true the writer is only allowed to write until the reader requested
	endln     int64
	ahead     spans
	readeroff int64 // offset where Read() last read
	writeroff int64 // file offset allowed for the writer in sync mode
	readed    int64 // size readed as bytes, useful for stats
	written   int64 // size written as bytes, useful for stats
	rerr      error
	werr      error
	eow       chan struct{} // end of writing
	eor       chan struct{} // end of reading
}

func newPipeFile(dirPath string) (*pipeFile, error) {
	file, err := ioutil.TempFile(dirPath, "pipefile")
	if err != nil {
		return nil, err
	}
	file, err = unlinkFile(file)
	if err != nil {
		return nil, err
	}
	f := &pipeFile{File: file,
		eow: make(chan struct{}),
		eor: make(chan struct{})}
	f.rCond.L = f.dataLock.RLocker() // Readers cond locker
	f.wCond.L = f.dataLock.RLocker() // Writers cond locker
	return f, nil
}

// waitForReadable waits until the requested data is available to read.
// We can stop waiting if the writer ended or if the reader is closed.
// The reader can be closed using Close() or CloseWithError we don't
// want to wait on a closed reader
func (f *pipeFile) waitForReadable(off int64) {
	f.dataLock.RLock()
	defer f.dataLock.RUnlock()

	for off >= f.endln {
		select {
		case <-f.eow:
			trace("eow")
			return
		case <-f.eor:
			trace("eor")
			return
		default:
			f.rCond.Wait()
		}
	}
}

func (f *pipeFile) updateReadedBytes(n int) {
	f.dataLock.Lock()
	defer f.dataLock.Unlock()
	f.readed += int64(n)
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
	close(f.eor)
	f.rCond.Broadcast()
	f.wCond.Broadcast()
}

// waitForWritable don't allow to write more than the reader requested.
// If the pipe is not in sync mode it does nothing.
// We can stop waiting it the reader need more data or if the reader or
// the writer are closed
func (f *pipeFile) waitForWritable() {
	if !f.syncMode {
		return
	}
	f.dataLock.RLock()
	defer f.dataLock.RUnlock()

	for f.endln > f.writeroff {
		select {
		case <-f.eow:
			trace("eow")
			return
		case <-f.eor:
			trace("eor")
			return
		default:
			f.wCond.Wait()
		}
	}
}

func (f *pipeFile) updateWrittenBytes(n int) {
	f.dataLock.Lock()
	defer f.dataLock.Unlock()
	f.written += int64(n)
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
	close(f.eow)
	f.rCond.Broadcast()
	f.wCond.Broadcast()
}

// set the new allowed write offset and signal the writers.
// Do nothing if not in sync mode
func (f *pipeFile) setWriteoff(off int64) {
	if !f.syncMode {
		return
	}
	f.dataLock.Lock()
	defer f.dataLock.Unlock()
	if off > f.writeroff {
		f.writeroff = off
		f.wCond.Broadcast()
	}
}

// PipeWriterAt is the io.WriterAt side of pipe.
type PipeWriterAt struct {
	f           *pipeFile
	asyncWriter bool
}

// PipeReaderAt is the io.ReaderAt side of pipe.
type PipeReaderAt struct {
	f *pipeFile
}

// Pipe creates an asynchronous file based pipe. It can be used to connect code
// expecting an io.ReaderAt with code expecting an io.WriterAt. Writes all go
// to an unlinked temporary file, reads start up as the file gets written up to
// their area. It is safe to call multiple ReadAt and WriteAt in parallel with
// each other.
func Pipe() (*PipeReaderAt, *PipeWriterAt, error) {
	return PipeInDir("")
}

// PipeInDir just like Pipe but the temporary file is created inside the specified
// directory
func PipeInDir(dirPath string) (*PipeReaderAt, *PipeWriterAt, error) {
	return newPipe(dirPath, false)
}

// AsyncWriterPipe is just like Pipe but the writer is allowed to close before
// the reader is finished. Whereas in Pipe the writer blocks until the reader
// is done.
func AsyncWriterPipe() (*PipeReaderAt, *PipeWriterAt, error) {
	return AsyncWriterPipeInDir("")
}

// AsyncWriterPipeInDir is just like AsyncWriterPipe but the temporary file is created
// inside the specified directory
func AsyncWriterPipeInDir(dirPath string) (*PipeReaderAt, *PipeWriterAt, error) {
	return newPipe(dirPath, true)
}

func newPipe(dirPath string, asyncWriter bool) (*PipeReaderAt, *PipeWriterAt, error) {
	fp, err := newPipeFile(dirPath)
	if err != nil {
		return nil, nil, err
	}
	fp.syncMode = !asyncWriter
	return &PipeReaderAt{fp}, &PipeWriterAt{fp, asyncWriter}, nil
}

// ReadAt implements the standard ReaderAt interface. It blocks if it gets
// ahead of the writer. You can call it from multiple threads.
func (r *PipeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	trace("readat", off)

	r.f.setWriteoff(off + int64(len(p)))
	r.f.waitForReadable(off + int64(len(p)))

	r.f.fileLock.RLock()
	defer r.f.fileLock.RUnlock()

	if err := r.f.readerror(); err != nil {
		trace("end readat(1):", off, 0, err)
		return 0, err
	}

	n, err := r.f.File.ReadAt(p, off)
	r.f.updateReadedBytes(n)
	if err != nil {
		if werr := r.f.writeerror(); werr != nil {
			err = werr
		}
	}
	trace("end readat(2):", off, n, err)
	return n, err
}

// It can also function as a io.Reader
func (r *PipeReaderAt) Read(p []byte) (int, error) {
	trace("read", len(p))
	n, err := r.ReadAt(p, r.f.readeroff)
	if n > 0 {
		r.f.dataLock.Lock()
		defer r.f.dataLock.Unlock()
		r.f.readeroff = r.f.readeroff + int64(n)
	}
	trace("end read", n, err)
	return n, err
}

// GetReadedBytes returns the bytes readed
func (r *PipeReaderAt) GetReadedBytes() int64 {
	r.f.dataLock.RLock()
	defer r.f.dataLock.RUnlock()
	return r.f.readed
}

// Close will Close the temp file and subsequent writes or reads will return an
// ErrClosePipe error.
func (r *PipeReaderAt) Close() error {
	return r.CloseWithError(nil)
}

// CloseWithError sets error and otherwise behaves like Close.
func (r *PipeReaderAt) CloseWithError(err error) error {
	if err == nil {
		err = io.EOF
	}
	r.f.fileLock.Lock()
	defer r.f.fileLock.Unlock()
	r.f.setReaderror(err)
	return r.f.File.Close()
}

// WriteAt implements the standard WriterAt interface. It will write to the
// temp file without blocking. You can call it from multiple threads.
func (w *PipeWriterAt) WriteAt(p []byte, off int64) (int, error) {
	//trace("writeat: ", string(p), off)
	//defer trace("wrote: ", string(p), off)

	w.f.waitForWritable()

	w.f.fileLock.RLock()
	defer w.f.fileLock.RUnlock()

	if err := w.f.writeerror(); err != nil {
		return 0, err
	}
	n, err := w.f.File.WriteAt(p, off)
	w.f.updateWrittenBytes(n)
	if err != nil {
		if err = w.f.readerror(); err != nil {
			return 0, err
		}
		return 0, io.EOF
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
		w.f.rCond.Broadcast()
	} else {
		w.f.ahead = append(w.f.ahead, span{off, off + int64(n)})
		sort.Sort(w.f.ahead) // should already be sorted..
	}
	// trace(w.f.ahead)
	return n, err
}

// Write provides a standard io.Writer interface.
func (w *PipeWriterAt) Write(p []byte) (int, error) {
	w.f.waitForWritable()
	w.f.fileLock.RLock()
	defer w.f.fileLock.RUnlock()
	n, err := w.f.Write(p)
	w.f.updateWrittenBytes(n)
	if n > 0 {
		w.f.dataLock.Lock()
		defer w.f.dataLock.Unlock()
		w.f.endln += int64(n)
		w.f.rCond.Broadcast()
	}
	return n, err
}

// GetWrittenBytes returns the bytes written
func (w *PipeWriterAt) GetWrittenBytes() int64 {
	w.f.dataLock.RLock()
	defer w.f.dataLock.RUnlock()
	return w.f.written
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
	// write is closed at this point, should I expose a way to check this?
	if !w.asyncWriter {
		w.WaitForReader()
	}
	return nil
}

// WaitForReader will block until the reader is closed. Returns the error set
// when the reader closed.
func (w *PipeWriterAt) WaitForReader() error {
	<-w.f.eor
	return w.f.readerror()
}

// debugging stuff
const watch = false

func trace(p ...interface{}) {
	if watch {
		fmt.Println(p...)
	}
}
