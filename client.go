// Package okq is a go client for the okq persitent queue
//
// To import inside your package do:
//
//	import "github.com/mediocregopher/okq-go.v2"
//
// Connecting
//
// Most of the time you'll want to use New to make a new Client. This will
// create a connection pool of the size given, and use that for all operation.
// Client's are thread-safe.
//
//	cl, err := okq.New("127.0.0.1:4777", 10)
//
// Pushing to queues
//
// All events in okq require a unique event id. This package will automatically
// generate a unique id if you use the standard Push methods.
//
//	cl.Push("super-queue", "my awesome event", okq.Normal)
//
// You can also create your own id by using the PushEvent methods. Remember
// though that the event id *must* be unique within that queue.
//
//	e := okq.Event{"super-queue", "unique id", "my awesome event"}
//	cl.PushEvent(&e, okq.Normal)
//
// Consuming from queues
//
// You can turn any Client into a consumer by using the Consumer methods. These
// will block as they call the given function on incoming events, and only
// return upon an error or a manual stop.
//
// Example of a consumer which should never quit
//	fn := func(e okq.Event) bool {
//		log.Printf("event received on %s: %s", e.Queue, e.Contents)
//		return true
//	}
//	for {
//		err := cl.Consumer(fn, nil, "queue1", "queue2")
//		log.Printf("error received from consumer: %s", err)
//	}
//
// See the doc string for the Consumer method for more details
package okq

import (
	"errors"
	"fmt"
	"time"

	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/pborman/uuid"
)

var uuidCh = make(chan string, 1024)

func init() {
	go func() {
		for {
			uuidCh <- uuid.New()
		}
	}()
}

// RedisPool is an interface which is implemented by radix.v2's pool.Pool type,
// but can be easily implemented by other types if desired
type RedisPool interface {
	Get() (*redis.Client, error)
	Put(*redis.Client)
	Empty()
}

// PushFlag is passed into either of the Push commands to alter their behavior.
// You can or multiple of these together to combine their behavior
type PushFlag int

const (
	// Normal is the expected behavior (call waits for event to be committed to
	// okq, normal priority)
	Normal PushFlag = 1 << iota

	// HighPriority causes the pushed event to be placed at the front of the
	// queue instead of the back
	HighPriority

	// NoBlock causes the server to not wait for the event to be committed to
	// disk before replying, it will reply as soon as it can and commit
	// asynchronously
	NoBlock
)

// DefaultTimeout is used as the default timeout for reading from the redis
// socket, and is used as the time to block per notify command for consumers.
// This is only relevant if using the the New function
var DefaultTimeout = 30 * time.Second

// Event is a single event which can be read from or written to an okq instance
type Event struct {
	Queue    string // The queue the event is coming from/going to
	ID       string // Unique id of this event
	Contents string // Arbitrary contents of the event
}

// IsZero returns true if this is an empty Event (usually used as a return from
// an empty queue)
func (e Event) IsZero() bool {
	return e == Event{}
}

func replyToEvent(q string, r *redis.Resp) (Event, error) {
	if r.IsType(redis.Nil) {
		return Event{}, nil
	}
	parts, err := r.List()
	if err != nil {
		return Event{}, err
	} else if len(parts) < 2 {
		return Event{}, errors.New("not enough elements in reply")
	}
	return Event{
		Queue:    q,
		ID:       parts[0],
		Contents: parts[1],
	}, nil
}

// Opts are various fields which can be used with NewWithOpts to create an okq
// client. Only RedisPool is required.
type Opts struct {
	RedisPool

	// Defaults to DefaultTimeout. This indicates the time a consumer should
	// block on the connection waiting for new events. This should be equal to
	// the read timeout on the redis connections.
	NotifyTimeout time.Duration
}

// Client is a client for the okq persistent queue which talks to a pool of okq
// instances.
//
// All methods on Client are thread-safe.
type Client struct {
	Opts

	closeCh chan bool
}

// NewWithOpts returns a new initialized Client based on the given Opts.
// RedisPool is a required field in Opts.
func NewWithOpts(o Opts) *Client {
	if o.NotifyTimeout == 0 {
		o.NotifyTimeout = DefaultTimeout
	}
	closeCh := make(chan bool)

	// Start up a routine which will periodically ping connections in the pool
	// to make sure they're alive. This ignores errors it gets, since its job is
	// to root out the bad eggs, so errors are expected.
	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-closeCh:
				return
			case <-t.C:
				c, err := o.RedisPool.Get()
				if err != nil {
					continue
				}
				c.Cmd("PING")
				o.RedisPool.Put(c)
			}
		}
	}()

	return &Client{
		Opts:    o,
		closeCh: closeCh,
	}
}

// New takes in a redis address and creates a connection pool for it.
// DefaultTimeout will be used for NotifyTimout.
func New(addr string, numConns int) (*Client, error) {
	df := func(network, addr string) (*redis.Client, error) {
		return redis.DialTimeout(network, addr, DefaultTimeout)
	}

	p, err := pool.NewCustom("tcp", addr, numConns, df)
	if err != nil {
		return nil, err
	}

	return NewWithOpts(Opts{
		RedisPool: p,
	}), nil
}

func (c *Client) cmd(cmd string, args ...interface{}) *redis.Resp {
	rclient, err := c.Get()
	if err != nil {
		return redis.NewResp(err)
	}
	defer c.Put(rclient)

	return rclient.Cmd(cmd, args...)
}

// PeekNext returns the next event which will be retrieved from the queue,
// without actually removing it from the queue. Returns an empty Event (IsZero()
// == true) if the queue is empty
func (c *Client) PeekNext(queue string) (Event, error) {
	return replyToEvent(queue, c.cmd("QRPEEK", queue))
}

// PeekLast returns the event most recently added to the queue, without actually
// removing it from the queue. Returns an empty Event (IsZero() == true) if the
// queue is empty
func (c *Client) PeekLast(queue string) (Event, error) {
	return replyToEvent(queue, c.cmd("QLPEEK", queue))
}

// PushEvent pushes the given event onto its queue. The event's Id must be
// unique within that queue
func (c *Client) PushEvent(e Event, f PushFlag) error {
	cmd := "QLPUSH"
	if f&HighPriority > 0 {
		cmd = "QRPUSH"
	}
	args := append(make([]interface{}, 0, 4), e.Queue, e.ID, e.Contents)
	if f&NoBlock > 0 {
		args = append(args, "NOBLOCK")
	}

	return c.cmd(cmd, args...).Err
}

// Push pushes an event with the given contents onto the queue. The event's ID
// will be an automatically generated uuid
//
// Normal event:
//
//	cl.Push("queue", "some event", okq.Normal)
//
// High priority event:
//
//	cl.Push("queue", "some important event", okq.HighPriority)
//
// Submit an event as fast as possible
//
//	cl.Push("queue", "not that important event", okq.NoBlock)
//
// Submit an important event, but do it as fast and unsafely as possibly (this
// probably would never actually be wanted
//
//	cl.Push("queue", "not that important event", okq.HighPriority & okq.NoBlock)
func (c *Client) Push(queue, contents string, f PushFlag) error {
	event := Event{Queue: queue, ID: <-uuidCh, Contents: contents}
	return c.PushEvent(event, f)
}

// QueueStatus describes the current status for a single queue, as described by
// the QSTATUS command
type QueueStatus struct {
	Name       string // Name of the queue
	Total      int64  // Total events in the queue, includes ones being processed
	Processing int64  // Number of events currently being processed
	Consumers  int64  // Number of connections registered as consumers for this queue
}

// Status returns the statuses of the given queues, or the statuses of all the
// known queues if no queues are given
func (c *Client) Status(queue ...string) ([]QueueStatus, error) {
	arr, err := c.cmd("QSTATUS", queue).Array()
	if err != nil {
		return nil, err
	}
	statuses := make([]QueueStatus, len(arr))
	for i := range arr {
		status, err := arr[i].Array()
		if err != nil {
			return nil, err
		} else if len(status) < 4 {
			return nil, fmt.Errorf("not enough elements in status: %s", status)
		}
		name, err := status[0].Str()
		if err != nil {
			return nil, err
		}
		total, err := status[1].Int64()
		if err != nil {
			return nil, err
		}
		processing, err := status[2].Int64()
		if err != nil {
			return nil, err
		}
		consumers, err := status[3].Int64()
		if err != nil {
			return nil, err
		}
		statuses[i] = QueueStatus{
			Name:       name,
			Total:      total,
			Processing: processing,
			Consumers:  consumers,
		}
	}
	return statuses, nil
}

// Close closes all connections that this client currently has pooled. Should
// only be called once all other commands and consumers are done running
func (c *Client) Close() error {
	// We don't close the closeCh, since we want to sync with the ping routine
	// to make sure it saw the close and isn't in the middle of pinging a
	// connection
	c.closeCh <- true
	c.Empty()
	return nil
}

// ConsumerFunc is passed into Consumer, and is used as a callback for incoming
// Events. It should return true if the event was processed successfully and
// false otherwise. If ConsumerUnsafe is being used the return is ignored
type ConsumerFunc func(Event) bool

// Consumer turns a client into a consumer. It will register itself on the given
// queues, and call the ConsumerFunc on all events it comes across. If stopCh is
// non-nil and is closed this will return ASAP.
//
// The ConsumerFunc is called synchronously, so if you wish to process events in
// parallel you'll have to all it multiple times from multiple go routines
func (c *Client) Consumer(
	fn ConsumerFunc, stopCh chan bool, queues ...string,
) error {
	return c.consumer(fn, stopCh, queues, false)
}

// ConsumerUnsafe is the same as Consumer except that the given ConsumerFunc is
// called asynchronously and its return value doesn't matter (because no QACK is
// ever sent to the okq server)
func (c *Client) ConsumerUnsafe(
	fn ConsumerFunc, stopCh chan bool, queues ...string,
) error {
	return c.consumer(fn, stopCh, queues, true)
}

func (c *Client) consumer(
	fn ConsumerFunc, stopCh chan bool, queues []string, noack bool,
) error {
	if len(queues) == 0 {
		return errors.New("no queues given to read from")
	}

	// Use slightly less than the actual read timeout on the tcp connections, so
	// otherwise there's a slight race condition
	notifyTimeout := int(time.Duration(float64(c.NotifyTimeout) * 0.9).Seconds())

	rclient, err := c.Get()
	if err != nil {
		return err
	}
	defer c.Put(rclient)

	if err := rclient.Cmd("QREGISTER", queues).Err; err != nil {
		return err
	}

	for {
		select {
		case <-stopCh:
			return nil
		default:
		}

		r := rclient.Cmd("QNOTIFY", notifyTimeout)
		if err := r.Err; err != nil {
			return err
		}

		if r.IsType(redis.Nil) {
			continue
		}

		q, err := r.Str()
		if err != nil {
			return err
		}

		args := []string{q}
		if noack {
			args = append(args, "EX", "0")
		}

		e, err := replyToEvent(q, rclient.Cmd("QRPOP", args))
		if err != nil {
			return err
		} else if e.IsZero() {
			continue
		}

		if noack {
			go fn(e)
			continue
		} else if fn(e) {
			// the return here doesn't matter, this is a best effort command. If
			// the connection is closed we'll find out on the next QNOTIFY
			// anyway
			rclient.Cmd("QACK", q, e.ID)
		}
	}
}
