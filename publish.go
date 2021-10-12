package rabbitmq

import (
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DeliveryMode. Transient means higher throughput but messages will not be
// restored on broker restart. The delivery mode of publishings is unrelated
// to the durability of the queues they reside on. Transient messages will
// not be restored to durable queues, persistent messages will be restored to
// durable queues and lost on non-durable queues during server restart.
//
// This remains typed as uint8 to match Publishing.DeliveryMode. Other
// delivery modes specific to custom queue implementations are not enumerated
// here.
const (
	Transient  uint8 = amqp.Transient
	Persistent uint8 = amqp.Persistent
)

// Return captures a flattened struct of fields returned by the server when a
// Publishing is unable to be delivered either due to the `mandatory` flag set
// and no route found, or `immediate` flag set and no free consumer.
type Return struct {
	amqp.Return
}

// Publisher allows you to publish messages safely across an open connection
type Publisher struct {
	chManager *channelManager

	notifyReturnChan chan Return

	disablePublishDueToFlow    bool
	disablePublishDueToFlowMux *sync.RWMutex

	exchangeName string

	logger Logger
}

// PublisherOptions are used to describe a publisher's configuration.
// Logging set to true will enable the consumer to print to stdout
type PublisherOptions struct {
	ExchangeOptions *ExchangeOptions
	Logging         bool
	Logger          Logger
}

// WithPublisherOptionsExchangeName returns a function that sets the exchange to publish to
func WithPublisherOptionsExchangeName(name string) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		getPublisherExchangeOptionsOrSetDefault(options).Name = name
	}
}

// WithPublisherOptionsExchangeKind returns a function that sets the binding exchange kind/type
func WithPublisherOptionsExchangeKind(kind string) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		getPublisherExchangeOptionsOrSetDefault(options).Kind = kind
	}
}

// WithPublisherOptionsExchangeDurable returns a function that sets the binding exchange durable flag
func WithPublisherOptionsExchangeDurable(options *PublisherOptions) {
	getPublisherExchangeOptionsOrSetDefault(options).Durable = true
}

// WithPublisherOptionsExchangeAutoDelete returns a function that sets the binding exchange autoDelete flag
func WithPublisherOptionsExchangeAutoDelete(options *PublisherOptions) {
	getPublisherExchangeOptionsOrSetDefault(options).AutoDelete = true
}

// WithPublisherOptionsExchangeInternal returns a function that sets the binding exchange internal flag
func WithPublisherOptionsExchangeInternal(options *PublisherOptions) {
	getPublisherExchangeOptionsOrSetDefault(options).Internal = true
}

// WithPublisherOptionsExchangeNoWait returns a function that sets the binding exchange noWait flag
func WithPublisherOptionsExchangeNoWait(options *PublisherOptions) {
	getPublisherExchangeOptionsOrSetDefault(options).NoWait = true
}

// WithPublisherOptionsExchangeArgs returns a function that sets the binding exchange arguments that are specific to the server's implementation of the exchange
func WithPublisherOptionsExchangeArgs(args Table) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		getPublisherExchangeOptionsOrSetDefault(options).ExchangeArgs = args
	}
}

// WithPublisherOptionsExchangeDeclare returns a function that declares the binding exchange.
// Use this setting if you want the consumer to create the exchange on start.
func WithPublisherOptionsExchangeDeclare(options *PublisherOptions) {
	getPublisherExchangeOptionsOrSetDefault(options).Declare = true
}

// WithPublisherOptionsLogging sets logging to true on the consumer options
func WithPublisherOptionsLogging(options *PublisherOptions) {
	options.Logging = true
	options.Logger = &stdLogger{}
}

// WithPublisherOptionsLogger sets logging to a custom interface.
// Use WithPublisherOptionsLogging to just log to stdout.
func WithPublisherOptionsLogger(log Logger) func(options *PublisherOptions) {
	return func(options *PublisherOptions) {
		options.Logging = true
		options.Logger = log
	}
}

// NewPublisher returns a new publisher with an open channel to the cluster.
// If you plan to enforce mandatory or immediate publishing, those failures will be reported
// on the channel of Returns that you should setup a listener on.
// Flow controls are automatically handled as they are sent from the server, and publishing
// will fail with an error when the server is requesting a slowdown
func NewPublisher(url string, config amqp.Config, optionFuncs ...func(*PublisherOptions)) (Publisher, error) {
	options := &PublisherOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.Logger == nil {
		options.Logger = &noLogger{} // default no logging
	}

	exchange := options.ExchangeOptions

	chManager, err := newChannelManager(url, config, options.Logger)
	if err != nil {
		return Publisher{}, err
	}

	publisher := Publisher{
		chManager:                  chManager,
		disablePublishDueToFlow:    false,
		disablePublishDueToFlowMux: &sync.RWMutex{},
		exchangeName:               exchange.Name,
		logger:                     options.Logger,
		notifyReturnChan:           nil,
	}

	if publisher.exchangeName == "" {
		publisher.logger.Printf("name not specified, publisher configured for default exchange")
	}

	if exchange.Declare {
		err = publisher.chManager.channel.ExchangeDeclare(
			exchange.Name,
			exchange.Kind,
			exchange.Durable,
			exchange.AutoDelete,
			exchange.Internal,
			exchange.NoWait,
			tableToAMQPTable(exchange.ExchangeArgs),
		)
	} else {
		err = publisher.chManager.channel.ExchangeDeclarePassive(
			exchange.Name,
			exchange.Kind,
			exchange.Durable,
			exchange.AutoDelete,
			exchange.Internal,
			exchange.NoWait,
			tableToAMQPTable(exchange.ExchangeArgs),
		)
	}

	if err != nil {
		return publisher, err
	}

	go publisher.startNotifyFlowHandler()

	// restart notifiers when cancel/close is triggered
	go func() {
		for err := range publisher.chManager.notifyCancelOrClose {
			publisher.logger.Printf("publish cancel/close handler triggered. err: %v", err)
			go publisher.startNotifyFlowHandler()
			if publisher.notifyReturnChan != nil {
				go publisher.startNotifyReturnHandler()
			}
		}
	}()

	return publisher, err
}

// NotifyReturn registers a listener for basic.return methods.
// These can be sent from the server when a publish is undeliverable either from the mandatory or immediate flags.
func (publisher *Publisher) NotifyReturn() <-chan Return {
	publisher.notifyReturnChan = make(chan Return)
	go publisher.startNotifyReturnHandler()
	return publisher.notifyReturnChan
}

// Publish publishes the provided data to the given routing keys over the connection
func (publisher *Publisher) Publish(
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) error {
	publisher.disablePublishDueToFlowMux.RLock()
	if publisher.disablePublishDueToFlow {
		return fmt.Errorf("publishing blocked due to high flow on the server")
	}
	publisher.disablePublishDueToFlowMux.RUnlock()

	options := &PublishOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.DeliveryMode == 0 {
		options.DeliveryMode = Transient
	}

	for _, routingKey := range routingKeys {
		var message = amqp.Publishing{}
		message.ContentType = options.ContentType
		message.DeliveryMode = options.DeliveryMode
		message.Body = data
		message.Headers = tableToAMQPTable(options.Headers)
		message.Expiration = options.Expiration

		// Actual publish.
		err := publisher.chManager.channel.Publish(
			publisher.exchangeName,
			routingKey,
			options.Mandatory,
			options.Immediate,
			message,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// StopPublishing stops the publishing of messages.
// The publisher should be discarded as it's not safe for re-use
func (publisher Publisher) StopPublishing() {
	publisher.chManager.channel.Close()
	publisher.chManager.connection.Close()
}

func (publisher *Publisher) startNotifyFlowHandler() {
	notifyFlowChan := publisher.chManager.channel.NotifyFlow(make(chan bool))
	publisher.disablePublishDueToFlowMux.Lock()
	publisher.disablePublishDueToFlow = false
	publisher.disablePublishDueToFlowMux.Unlock()

	// Listeners for active=true flow control.  When true is sent to a listener,
	// publishing should pause until false is sent to listeners.
	for ok := range notifyFlowChan {
		publisher.disablePublishDueToFlowMux.Lock()
		if ok {
			publisher.logger.Printf("pausing publishing due to flow request from server")
			publisher.disablePublishDueToFlow = true
		} else {
			publisher.disablePublishDueToFlow = false
			publisher.logger.Printf("resuming publishing due to flow request from server")
		}
		publisher.disablePublishDueToFlowMux.Unlock()
	}
}

func (publisher *Publisher) startNotifyReturnHandler() {
	returnAMQPCh := publisher.chManager.channel.NotifyReturn(make(chan amqp.Return, 1))
	for ret := range returnAMQPCh {
		publisher.notifyReturnChan <- Return{ret}
	}
}
