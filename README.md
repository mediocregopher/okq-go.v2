# okq-go

[![GoDoc](https://godoc.org/github.com/mediocregopher/okq-go.v2?status.svg)](https://godoc.org/github.com/mediocregopher/okq-go.v2)

**This package is still under development, and its API may change at any time.
With that said, it is also being used in production, so API changes aside it
should always be usable. The [previous
version](https://github.com/mediocregopher/okq-go) is completely stable.**

A go driver for the [okq](https://github.com/mc0/okq) persistent queue.

okq uses the redis protocol and calling conventions as it's interface, so any
standard redis client can be used to interact with it. This package wraps around
a normal redis pool, however, to provide a convenient interface to interfact
with. Specifically, it creates a simple interface for event consumers to use so
they retrieve events through a channel rather than implementing that logic
manually everytime.

## Usage

To get:

    go get github.com/mediocregopher/okq-go.v2

To import:

    import "github.com/mediocregopher/okq-go.v2"
