package dbr

type EventReceiver interface {
	Event(eventName string)
	EventKv(eventName string, kvs map[string]string)
	EventErr(eventName string, err error) error
	EventErrKv(eventName string, err error, kvs map[string]string) error
	Timing(eventName string, nanoseconds int64)
	TimingKv(eventName string, nanoseconds int64, kvs map[string]string)
}

type kvs map[string]string

//
// Implement a sentinel event receiver. If a caller doesn't want to supply an event receiver, then we'll use an instance of this:
//
type NullEventReceiver struct{}

var nullReceiver = &NullEventReceiver{}

func (n *NullEventReceiver) Event(eventName string) {
	// noop
}

func (n *NullEventReceiver) EventKv(eventName string, kvs map[string]string) {
	// noop
}

func (n *NullEventReceiver) EventErr(eventName string, err error) error {
	return err
}

func (n *NullEventReceiver) EventErrKv(eventName string, err error, kvs map[string]string) error {
	return err
}

func (n *NullEventReceiver) Timing(eventName string, nanoseconds int64) {
	// noop
}

func (n *NullEventReceiver) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) {
	// noop
}