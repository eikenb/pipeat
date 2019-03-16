PipeAt
======

Works like io.Pipe() but allows use of ReadAt/WriteAt and asynchronous
operations. Useful for connecting IO pipelines where one or both ends require
offset based file access.

[![GoDoc](http://godoc.org/github.com/eikenb/pipeat?status.svg)](http://godoc.org/github.com/eikenb/pipeat) [![Build Status](https://travis-ci.org/eikenb/pipeat.svg?branch=master)](https://travis-ci.org/eikenb/pipeat)

