package rabbitmq

// getDefaultConsumeOptions descibes the options that will be used when a value isn't provided
func getDefaultConsumeOptions() ConsumeOptions {
	return ConsumeOptions{
		QueueDurable:      false,
		QueueAutoDelete:   false,
		QueueExclusive:    false,
		QueueNoWait:       false,
		QueueDeclare:      true,
		QueueArgs:         nil,
		BindingNoWait:     false,
		BindingArgs:       nil,
		ExchangeOptions:   nil,
		Concurrency:       1,
		QOSPrefetch:       0,
		QOSGlobal:         false,
		ConsumerName:      "",
		ConsumerAutoAck:   false,
		ConsumerExclusive: false,
		ConsumerNoWait:    false,
		ConsumerNoLocal:   false,
		ConsumerArgs:      nil,
	}
}

// ConsumeOptions are used to describe how a new consumer will be created.
type ConsumeOptions struct {
	QueueDurable      bool
	QueueAutoDelete   bool
	QueueExclusive    bool
	QueueNoWait       bool
	QueueDeclare      bool
	QueueArgs         Table
	BindingNoWait     bool
	BindingArgs       Table
	Concurrency       int
	ExchangeOptions   *ExchangeOptions
	QOSPrefetch       int
	QOSGlobal         bool
	ConsumerName      string
	ConsumerAutoAck   bool
	ConsumerExclusive bool
	ConsumerNoWait    bool
	ConsumerNoLocal   bool
	ConsumerArgs      Table
}

// WithConsumeOptionsQueueDurable sets the queue to durable, which means it won't
// be destroyed when the server restarts. It must only be bound to durable exchanges
func WithConsumeOptionsQueueDurable(options *ConsumeOptions) {
	options.QueueDurable = true
}

// WithConsumeOptionsQueueAutoDelete sets the queue to auto delete, which means it will
// be deleted when there are no more conusmers on it
func WithConsumeOptionsQueueAutoDelete(options *ConsumeOptions) {
	options.QueueAutoDelete = true
}

// WithConsumeOptionsQueueExclusive sets the queue to exclusive, which means
// it's are only accessible by the connection that declares it and
// will be deleted when the connection closes. Channels on other connections
// will receive an error when attempting to declare, bind, consume, purge or
// delete a queue with the same name.
func WithConsumeOptionsQueueExclusive(options *ConsumeOptions) {
	options.QueueExclusive = true
}

// WithConsumeOptionsQueueNoWait sets the queue to nowait, which means
// the queue will assume to be declared on the server.  A
// channel exception will arrive if the conditions are met for existing queues
// or attempting to modify an existing queue from a different connection.
func WithConsumeOptionsQueueNoWait(options *ConsumeOptions) {
	options.QueueNoWait = true
}

// WithConsumeOptionsQueueNoDeclare sets the queue to no declare, which means
// the queue will be assumed to be declared on the server, and won't be
// declared at all.
func WithConsumeOptionsQueueNoDeclare(options *ConsumeOptions) {
	options.QueueDeclare = false
}

// WithConsumeOptionsQuorum sets the queue a quorum type, which means multiple nodes
// in the cluster will have the messages distributed amongst them for higher reliability
func WithConsumeOptionsQuorum(options *ConsumeOptions) {
	if options.QueueArgs == nil {
		options.QueueArgs = Table{}
	}
	options.QueueArgs["x-queue-type"] = "quorum"
}

// WithConsumeOptionsBindingExchangeName returns a function that sets the exchange name the queue will be bound to
func WithConsumeOptionsBindingExchangeName(name string) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		getConsumerExchangeOptionsOrSetDefault(options).Name = name
	}
}

// WithConsumeOptionsBindingExchangeKind returns a function that sets the binding exchange kind/type
func WithConsumeOptionsBindingExchangeKind(kind string) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		getConsumerExchangeOptionsOrSetDefault(options).Kind = kind
	}
}

// WithConsumeOptionsBindingExchangeDurable returns a function that sets the binding exchange durable flag
func WithConsumeOptionsBindingExchangeDurable(options *ConsumeOptions) {
	getConsumerExchangeOptionsOrSetDefault(options).Durable = true
}

// WithConsumeOptionsBindingExchangeAutoDelete returns a function that sets the binding exchange autoDelete flag
func WithConsumeOptionsBindingExchangeAutoDelete(options *ConsumeOptions) {
	getConsumerExchangeOptionsOrSetDefault(options).AutoDelete = true
}

// WithConsumeOptionsBindingExchangeInternal returns a function that sets the binding exchange internal flag
func WithConsumeOptionsBindingExchangeInternal(options *ConsumeOptions) {
	getConsumerExchangeOptionsOrSetDefault(options).Internal = true
}

// WithConsumeOptionsBindingExchangeNoWait returns a function that sets the binding exchange noWait flag
func WithConsumeOptionsBindingExchangeNoWait(options *ConsumeOptions) {
	getConsumerExchangeOptionsOrSetDefault(options).NoWait = true
}

// WithConsumeOptionsBindingExchangeArgs returns a function that sets the binding exchange arguments that are specific to the server's implementation of the exchange
func WithConsumeOptionsBindingExchangeArgs(args Table) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		getConsumerExchangeOptionsOrSetDefault(options).ExchangeArgs = args
	}
}

// WithConsumeOptionsBindingExchangeDeclare returns a function that declares the binding exchange.
// Use this setting if you want the consumer to create the exchange on start.
func WithConsumeOptionsBindingExchangeDeclare(options *ConsumeOptions) {
	getConsumerExchangeOptionsOrSetDefault(options).Declare = true
}

// WithConsumeOptionsBindingNoWait sets the bindings to nowait, which means if the queue can not be bound
// the channel will not be closed with an error.
func WithConsumeOptionsBindingNoWait(options *ConsumeOptions) {
	options.BindingNoWait = true
}

// WithConsumeOptionsConcurrency returns a function that sets the concurrency, which means that
// many goroutines will be spawned to run the provided handler on messages
func WithConsumeOptionsConcurrency(concurrency int) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.Concurrency = concurrency
	}
}

// WithConsumeOptionsQOSPrefetch returns a function that sets the prefetch count, which means that
// many messages will be fetched from the server in advance to help with throughput.
// This doesn't affect the handler, messages are still processed one at a time.
func WithConsumeOptionsQOSPrefetch(prefetchCount int) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.QOSPrefetch = prefetchCount
	}
}

// WithConsumeOptionsQOSGlobal sets the qos on the channel to global, which means
// these QOS settings apply to ALL existing and future
// consumers on all channels on the same connection
func WithConsumeOptionsQOSGlobal(options *ConsumeOptions) {
	options.QOSGlobal = true
}

// WithConsumeOptionsConsumerName returns a function that sets the name on the server of this consumer
// if unset a random name will be given
func WithConsumeOptionsConsumerName(consumerName string) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.ConsumerName = consumerName
	}
}

// WithConsumeOptionsConsumerAutoAck returns a function that sets the auto acknowledge property on the server of this consumer
// if unset the default will be used (false)
func WithConsumeOptionsConsumerAutoAck(autoAck bool) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.ConsumerAutoAck = autoAck
	}
}

// WithConsumeOptionsConsumerExclusive sets the consumer to exclusive, which means
// the server will ensure that this is the sole consumer
// from this queue. When exclusive is false, the server will fairly distribute
// deliveries across multiple consumers.
func WithConsumeOptionsConsumerExclusive(options *ConsumeOptions) {
	options.ConsumerExclusive = true
}

// WithConsumeOptionsConsumerNoWait sets the consumer to nowait, which means
// it does not wait for the server to confirm the request and
// immediately begin deliveries. If it is not possible to consume, a channel
// exception will be raised and the channel will be closed.
func WithConsumeOptionsConsumerNoWait(options *ConsumeOptions) {
	options.ConsumerNoWait = true
}

// WithConsumeOptionsQueueArgs returns a function that sets the queue arguments
func WithConsumeOptionsQueueArgs(args Table) func(*ConsumeOptions) {
	return func(options *ConsumeOptions) {
		options.QueueArgs = args
	}
}
