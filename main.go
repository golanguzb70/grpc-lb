package grpclb

import (
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type LB interface {
	Get() *grpc.ClientConn
	Close() error
}

type lb struct {
	conns                   []*grpc.ClientConn
	size                    uint32
	offset                  uint32
	factory                 func() (*grpc.ClientConn, error)
	mutex                   sync.Mutex
	lastReset               time.Time
	minRetryIntervalSeconds uint32
	logger                  func(msg string)
	useCount                uint64
}

/*
New creates a new load balancer with the given factory function and size.
The factory function is used to create the connections that the load balancer
will manage. The size parameter determines how many connections the load
balancer will manage. The factory function must return a new connection each
time it is called. The size parameter must be greater than 0.
*/
func New(size uint32, minRetryIntervalSeconds uint32, factory func() (*grpc.ClientConn, error), logger func(msg string)) (LB, error) {
	switch {
	case factory == nil:
		return nil, errors.New("factory can't be nil3")
	case size <= 0:
		return nil, errors.New("size must be greater than 0")
	case minRetryIntervalSeconds <= 0:
		return nil, errors.New("minRetryIntervalSeconds must be greater than 0")
	}

	conns := make([]*grpc.ClientConn, size)
	for i := uint32(0); i < size; i++ {
		conn, err := factory()
		if err != nil {
			return nil, err
		}

		conns[i] = conn
	}

	return &lb{
		conns:                   conns,
		size:                    size,
		offset:                  0,
		factory:                 factory,
		mutex:                   sync.Mutex{},
		lastReset:               time.Now().UTC(),
		minRetryIntervalSeconds: minRetryIntervalSeconds,
		logger:                  logger,
		useCount:                0,
	}, nil
}

/*
Get returns the next connection managed by the load balancer. The connections
are returned in a round-robin fashion. If a connection is not ready, the next
connection is returned. If all connections are not ready, the connections are
reset and the first connection is returned. If the connections fail to reset,
nil is returned.
*/
func (o *lb) Get() *grpc.ClientConn {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	conn := o.conns[o.offset]

	if conn.GetState() != connectivity.Ready && o.useCount > uint64(o.offset) {
		if time.Now().UTC().Sub(o.lastReset) > time.Duration(o.minRetryIntervalSeconds)*time.Second {
			o.lastReset = time.Now().UTC()
			if err := o.reset(); err != nil {
				if o.logger != nil {
					o.logger("Failed to reset connections: " + err.Error())
				}
				return nil
			}

			conn = o.conns[o.offset]
		}
	}

	o.offset = (o.offset + 1) % o.size
	o.useCount++
	return conn
}

/*
Close closes all the connections managed by the load balancer. If any of the
connections fail to close, an error is returned.
*/
func (o *lb) Close() error {
	for _, conn := range o.conns {
		if err := conn.Close(); err != nil {
			return err
		}
	}

	return nil
}

/*
Reset closes all the connections managed by the load balancer and creates new
connections using the factory function. If any of the connections fail to close
or if any of the new connections fail to be created, an error is returned.
*/
func (o *lb) reset() error {
	for i := uint32(0); i < o.size; i++ {
		if err := o.conns[i].Close(); err != nil {
			return err
		}

		conn, err := o.factory()
		if err != nil {
			return err
		}

		o.conns[i] = conn
	}

	return nil
}
