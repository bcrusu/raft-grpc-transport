package transport

import "time"

type Option func(m *Manager)

type Logger func(err error, msg string, args ...interface{})

// WithHeartbeatTimeout configures the transport to not wait for more than d
// for a heartbeat to be executed by a remote peer.
func WithHeartbeatTimeout(d time.Duration) Option {
	return func(m *Manager) {
		m.heartbeatTimeout = d
	}
}

// WithErrorLogger configures a simple logging utility for errors.
func WithErrorLogger(logger Logger) Option {
	return func(m *Manager) {
		m.logger = logger
	}
}

func nilLogger(err error, msg string, args ...interface{}) {}
