package stream

import (
	"context"
	"fmt"
	"github.com/applike/gosoline/pkg/cfg"
	"github.com/applike/gosoline/pkg/coffin"
	"github.com/applike/gosoline/pkg/kernel"
	"github.com/applike/gosoline/pkg/mon"
	"github.com/applike/gosoline/pkg/tracing"
	"sync"
	"time"
)

//go:generate mockery -name=BatchConsumerCallback
type BatchConsumerCallback interface {
	Boot(config cfg.Config, logger mon.Logger) error
	GetModel(attributes map[string]interface{}) interface{}
	Consume(ctx context.Context, models []*Message) (bool, error)
}

type BaseBatchConsumer struct {
	kernel.EssentialModule
	kernel.ApplicationStage

	logger mon.Logger
	tracer tracing.Tracer

	input  Input
	cfn    coffin.Coffin
	ticker *time.Ticker

	name     string
	callback BatchConsumerCallback

	m         sync.Mutex
	processed int
	batchSize int
	batch     []*Message
	force     bool
}

func NewBatchConsumer(name string, callback BatchConsumerCallback) *BaseBatchConsumer {
	return &BaseBatchConsumer{
		name:     name,
		callback: callback,
	}
}

func (c *BaseBatchConsumer) Boot(config cfg.Config, logger mon.Logger) error {
	if err := c.bootCallback(config, logger); err != nil {
		return err
	}

	tracer := tracing.ProviderTracer(config, logger)
	cfn := coffin.New()

	batchSize := config.GetInt("consumer_batch_size")
	idleTimeout := config.GetDuration("consumer_idle_timeout")
	ticker := time.NewTicker(idleTimeout * time.Second)

	c.BootWithInterfaces(logger, tracer, cfn, batchSize, ticker)

	return nil
}

func (c *BaseBatchConsumer) BootWithInterfaces(logger mon.Logger, tracer tracing.Tracer, cfn coffin.Coffin, batchSize int, ticker *time.Ticker) {
	c.logger = logger
	c.tracer = tracer
	c.cfn = cfn
	c.batchSize = batchSize
	c.batch = make([]*Message, 0, batchSize)
	c.ticker = ticker
}

func (c *BaseBatchConsumer) Run(ctx context.Context) error {
	defer c.logger.Info("leaving consumer ", c.name)

	c.cfn.GoWithContextf(ctx, c.input.Run, "panic during run of the consumer input")
	c.cfn.Gof(c.consume, "panic during consuming")

	for {
		select {
		case <-ctx.Done():
			c.input.Stop()
			return c.cfn.Wait()

		case <-c.cfn.Dead():
			return c.cfn.Err()

		case <-c.ticker.C:
			c.consumeBatch()

			c.logger.Info(fmt.Sprintf("processed %v messages", c.processed))
			c.processed = 0
		}
	}
}

func (c *BaseBatchConsumer) consume() error {
	for {
		msg, ok := <-c.input.Data()

		if !ok {
			return nil
		}

		c.m.Lock()
		c.batch = append(c.batch, msg)
		c.m.Unlock()

		if len(c.batch) < c.batchSize {
			continue
		}

		c.consumeBatch()
	}
}

func (c *BaseBatchConsumer) consumeBatch() {
	if len(c.batch) == 0 {
		return
	}

	ctx := context.Background()

	c.m.Lock()
	defer c.m.Unlock()

	ack, err := c.callback.Consume(ctx, c.batch)

	if err != nil {
		c.logger.Error(err, "an error occurred during the consume operation")
	}

	if !ack {
		return
	}

	c.AcknowledgeBatch(ctx, c.batch)

	c.processed += len(c.batch)
	c.batch = make([]*Message, 0, c.batchSize)
}

func (c *BaseBatchConsumer) bootCallback(config cfg.Config, logger mon.Logger) error {
	loggerCallback := logger.WithChannel("callback")
	contextEnforcingLogger := mon.NewContextEnforcingLogger(loggerCallback)

	err := c.callback.Boot(config, contextEnforcingLogger)

	if err != nil {
		return fmt.Errorf("error during booting the consumer callback: %w", err)
	}

	contextEnforcingLogger.Enable()

	return nil
}

func (c *BaseBatchConsumer) AcknowledgeBatch(ctx context.Context, msg []*Message) {
	var ok bool
	var ackInput AcknowledgeableInput

	if ackInput, ok = c.input.(AcknowledgeableInput); !ok {
		return
	}

	err := ackInput.AckBatch(msg)

	if err != nil {
		c.logger.WithContext(ctx).Error(err, "could not acknowledge the messages")
	}
}