package okq

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	DefaultTimeout = 2 * time.Second
}

func randString() string {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func testClient() *Client {
	c, err := New("localhost:4777", 1)
	if err != nil {
		panic(err)
	}
	return c
}

func TestClient(t *T) {
	assert := assert.New(t)
	require := require.New(t)

	c := testClient()
	q := randString()

	// Queue is currently empty, make sure PeekNext and PeekLast return nil
	e, err := c.PeekNext(q)
	require.Nil(err)
	assert.True(e.IsZero())

	e, err = c.PeekLast(q)
	require.Nil(err)
	assert.True(e.IsZero())

	// Add some items to the queue
	require.Nil(c.Push(q, "foo", Normal))
	require.Nil(c.Push(q, "bar", Normal))
	require.Nil(c.Push(q, "baz", HighPriority))

	// The queue should now be (from first to last) "baz", "foo", "bar"
	e, err = c.PeekNext(q)
	require.Nil(err)
	assert.Equal(e.Contents, "baz")

	e, err = c.PeekLast(q)
	require.Nil(err)
	assert.Equal(e.Contents, "bar")

	statuses, err := c.Status(q)
	require.Nil(err)
	assert.Equal(statuses[0], QueueStatus{q, 3, 0, 0})
}

func TestConsumer(t *T) {
	assert := assert.New(t)
	require := require.New(t)

	c1, c2 := testClient(), testClient()
	q := randString()

	stopCh := make(chan bool)
	workCh := make(chan bool)

	i := 0
	fn := func(e Event) bool {
		assert.Equal(q, e.Queue)
		assert.Equal(strconv.Itoa(i), e.Contents)
		i++
		workCh <- true
		return true
	}

	retCh := make(chan error)
	go func() {
		retCh <- c1.Consumer(fn, stopCh, q)
	}()

	for i := 0; i < 1000; i++ {
		require.Nil(c2.Push(q, strconv.Itoa(i), Normal))
		<-workCh
	}

	close(stopCh)
	require.Nil(<-retCh)
	require.Nil(c1.Close())

	statuses, err := c2.Status(q)
	require.Nil(err)
	assert.Equal(QueueStatus{q, 0, 0, 0}, statuses[0])
}
