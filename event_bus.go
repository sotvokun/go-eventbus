package eventbus

import (
	"fmt"
	"reflect"
	"sync"
)

// BusSubscriber defines subscription-related bus behavior
type BusSubscriber interface {
	Subscribe(topic string, fn any) error
	SubscribeAsync(topic string, fn any, transactional bool) error
	SubscribeOnce(topic string, fn any) error
	SubscribeOnceAsync(topic string, fn any) error
	Unsubscribe(topic string, handler any) error
	SetArgumentProcessor(topic string, argProc ArgumentProcessor)
	SetDefaultArgumentProcessor(argProc ...ArgumentProcessor)
}

// BusPublisher defines publishing-related bus behavior
type BusPublisher interface {
	Publish(topic string, args ...any)
}

// BusController defines bus control behavior (checking handler's presence, synchronization)
type BusController interface {
	HasCallback(topic string) bool
	WaitAsync()
}

// Bus englobes global (subscribe, publish, control) bus behavior
type Bus interface {
	BusController
	BusSubscriber
	BusPublisher
}

type ArgumentProcessor = func(callback *EventHandler, arg ...any) []reflect.Value

// EventBus - box for handlers and callbacks.
type EventBus struct {
	handlers       map[string][]*EventHandler
	wg             sync.WaitGroup
	lock           sync.Mutex // a lock for the map
	argProcs       map[string]ArgumentProcessor
	defaultArgProc ArgumentProcessor
}

type EventHandler struct {
	Callback      reflect.Value
	once          *sync.Once
	async         bool
	transactional bool
	sync.Mutex    // lock for an event handler - useful for running async callbacks serially
}

// New returns new EventBus with empty handlers.
func New() Bus {
	b := &EventBus{
		make(map[string][]*EventHandler),
		sync.WaitGroup{},
		sync.Mutex{},
		make(map[string]ArgumentProcessor),
		nil,
	}
	b.SetDefaultArgumentProcessor()
	return Bus(b)
}

// doSubscribe handles the subscription logic and is utilized by the public Subscribe functions
func (bus *EventBus) doSubscribe(topic string, fn any, handler *EventHandler) error {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	if !(reflect.TypeOf(fn).Kind() == reflect.Func) {
		return fmt.Errorf("%s is not of type reflect.Func", reflect.TypeOf(fn).Kind())
	}
	bus.handlers[topic] = append(bus.handlers[topic], handler)
	return nil
}

// Subscribe subscribes to a topic.
// Returns error if `fn` is not a function.
func (bus *EventBus) Subscribe(topic string, fn any) error {
	return bus.doSubscribe(topic, fn, &EventHandler{
		reflect.ValueOf(fn), nil, false, false, sync.Mutex{},
	})
}

// SubscribeAsync subscribes to a topic with an asynchronous callback
// Transactional determines whether subsequent callbacks for a topic are
// run serially (true) or concurrently (false)
// Returns error if `fn` is not a function.
func (bus *EventBus) SubscribeAsync(topic string, fn any, transactional bool) error {
	return bus.doSubscribe(topic, fn, &EventHandler{
		reflect.ValueOf(fn), nil, true, transactional, sync.Mutex{},
	})
}

// SubscribeOnce subscribes to a topic once. Handler will be removed after executing.
// Returns error if `fn` is not a function.
func (bus *EventBus) SubscribeOnce(topic string, fn any) error {
	return bus.doSubscribe(topic, fn, &EventHandler{
		reflect.ValueOf(fn), new(sync.Once), false, false, sync.Mutex{},
	})
}

// SubscribeOnceAsync subscribes to a topic once with an asynchronous callback
// Handler will be removed after executing.
// Returns error if `fn` is not a function.
func (bus *EventBus) SubscribeOnceAsync(topic string, fn any) error {
	return bus.doSubscribe(topic, fn, &EventHandler{
		reflect.ValueOf(fn), new(sync.Once), true, false, sync.Mutex{},
	})
}

// HasCallback returns true if exists any callback subscribed to the topic.
func (bus *EventBus) HasCallback(topic string) bool {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	_, ok := bus.handlers[topic]
	if ok {
		return len(bus.handlers[topic]) > 0
	}
	return false
}

// Unsubscribe removes callback defined for a topic.
// Returns error if there are no callbacks subscribed to the topic.
func (bus *EventBus) Unsubscribe(topic string, handler any) error {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	if _, ok := bus.handlers[topic]; ok && len(bus.handlers[topic]) > 0 {
		bus.removeHandler(topic, bus.findHandlerIdx(topic, reflect.ValueOf(handler)))
		return nil
	}
	return fmt.Errorf("topic %s doesn't exist", topic)
}

// Publish executes callback defined for a topic. Any additional argument will be transferred to the callback.
func (bus *EventBus) Publish(topic string, args ...any) {
	// Handlers slice may be changed by removeHandler and Unsubscribe during iteration,
	// so make a copy and iterate the copied slice.
	bus.lock.Lock()
	handlers := bus.handlers[topic]
	copyHandlers := make([]*EventHandler, len(handlers))
	copy(copyHandlers, handlers)
	bus.lock.Unlock()
	for _, handler := range copyHandlers {
		if !handler.async {
			bus.doPublish(handler, topic, args...)
		} else {
			bus.wg.Add(1)
			if handler.transactional {
				handler.Lock()
			}
			go bus.doPublishAsync(handler, topic, args...)
		}
	}
}

func (bus *EventBus) doPublish(handler *EventHandler, topic string, args ...any) {
	argProc, ok := bus.argProcs[topic]
	if !ok {
		argProc = bus.defaultArgProc
	}
	passedArguments := argProc(handler, args...)
	if handler.once == nil {
		handler.Callback.Call(passedArguments)
	} else {
		handler.once.Do(func() {
			bus.lock.Lock()
			for idx, h := range bus.handlers[topic] {
				// compare pointers since pointers are unique for all members of slice
				if h.once == handler.once {
					bus.removeHandler(topic, idx)
					break
				}
			}
			bus.lock.Unlock()
			handler.Callback.Call(passedArguments)
		})
	}
}

func (bus *EventBus) doPublishAsync(handler *EventHandler, topic string, args ...any) {
	defer bus.wg.Done()
	if handler.transactional {
		defer handler.Unlock()
	}
	bus.doPublish(handler, topic, args...)
}

func (bus *EventBus) removeHandler(topic string, idx int) {
	if _, ok := bus.handlers[topic]; !ok {
		return
	}
	l := len(bus.handlers[topic])

	if !(0 <= idx && idx < l) {
		return
	}

	copy(bus.handlers[topic][idx:], bus.handlers[topic][idx+1:])
	bus.handlers[topic][l-1] = nil // or the zero value of T
	bus.handlers[topic] = bus.handlers[topic][:l-1]
}

func (bus *EventBus) findHandlerIdx(topic string, callback reflect.Value) int {
	if _, ok := bus.handlers[topic]; ok {
		for idx, handler := range bus.handlers[topic] {
			if handler.Callback.Type() == callback.Type() &&
				handler.Callback.Pointer() == callback.Pointer() {
				return idx
			}
		}
	}
	return -1
}

func (bus *EventBus) setupArguments(handler *EventHandler, args ...any) []reflect.Value {
	funcType := handler.Callback.Type()
	passedArguments := make([]reflect.Value, len(args))
	for i, v := range args {
		if v == nil {
			passedArguments[i] = reflect.New(funcType.In(i)).Elem()
		} else {
			passedArguments[i] = reflect.ValueOf(v)
		}
	}

	return passedArguments
}

// WaitAsync waits for all async callbacks to complete
func (bus *EventBus) WaitAsync() {
	bus.wg.Wait()
}

// SetArgumentProcessor sets the argument processor for a topic
//
// The argument processor is useful for customizing how arguments are passed
// to the subscriber's callback. The default argument processor will be used
// if no argument processor is set for a topic.
func (bus *EventBus) SetArgumentProcessor(topic string, argProc ArgumentProcessor) {
	bus.argProcs[topic] = argProc
}

// SetDefaultArgumentProcessor sets the default argument processor
//
// Pass no arguments to reset the default argument processor to the built-in
// default argument processor.
func (bus *EventBus) SetDefaultArgumentProcessor(argProc ...ArgumentProcessor) {
	if len(argProc) > 0 {
		bus.defaultArgProc = argProc[0]
	} else {
		bus.defaultArgProc = bus.setupArguments
	}
}
