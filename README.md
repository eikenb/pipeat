PipeAt
======

Works like io.Pipe() but allows use of ReadAt/WriteAt and asynchronous
operations. Useful for connecting IO pipelines where one or both ends require
offset based file access.

[![GoDoc](http://godoc.org/github.com/eikenb/pipeat?status.svg)](http://godoc.org/github.com/eikenb/pipeat)
[![Build Status](https://github.com/eikenb/pipeat/actions/workflows/ci.yml/badge.svg)](https://github.com/eikenb/pipeat/actions)

