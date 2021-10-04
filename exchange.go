package rabbitmq

// ExchangeOptions are used when configuring or binding to an exchange.
// it will verify the exchange is created before binding to it.
type ExchangeOptions struct {
	Name         string
	Kind         string
	Durable      bool
	AutoDelete   bool
	Internal     bool
	NoWait       bool
	ExchangeArgs Table
	Declare      bool
}

// getConsumerExchangeOptionsOrSetDefault returns pointer to current Exchange options. if no Exchange options are set yet, it will set it with default values.
func getConsumerExchangeOptionsOrSetDefault(options *ConsumeOptions) *ExchangeOptions {
	if options.ExchangeOptions == nil {
		options.ExchangeOptions = &ExchangeOptions{
			Name:         "",
			Kind:         "direct",
			Durable:      false,
			AutoDelete:   false,
			Internal:     false,
			NoWait:       false,
			ExchangeArgs: nil,
			Declare:      false,
		}
	}
	return options.ExchangeOptions
}

// getPublisherExchangeOptionsOrSetDefault returns pointer to current Exchange options. if no Exchange options are set yet, it will set it with default values.
func getPublisherExchangeOptionsOrSetDefault(options *PublisherOptions) *ExchangeOptions {
	if options.ExchangeOptions == nil {
		options.ExchangeOptions = &ExchangeOptions{
			Name:         "",
			Kind:         "direct",
			Durable:      false,
			AutoDelete:   false,
			Internal:     false,
			NoWait:       false,
			ExchangeArgs: nil,
			Declare:      true,
		}
	}
	return options.ExchangeOptions
}
