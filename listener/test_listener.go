package listener

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/streadway/amqp"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/rabbitmq"
	"go.opentelemetry.io/otel"
	"slices"
	"time"
)

type TestEventAccumulator struct {
	cfg          *config.AppConfig
	lgr          *logger.LoggerWrapper
	eventsBuffer []*dto.GlobalUserEvent
}

func (p *TestEventAccumulator) OnEvent(ctx context.Context, e *dto.GlobalUserEvent) {
	p.eventsBuffer = append(p.eventsBuffer, e)
}

func NewTetsEventAccumulator(cfg *config.AppConfig, lgr *logger.LoggerWrapper) *TestEventAccumulator {
	return &TestEventAccumulator{
		cfg:          cfg,
		lgr:          lgr,
		eventsBuffer: make([]*dto.GlobalUserEvent, 0),
	}
}

func (p *TestEventAccumulator) Clean() {
	p.eventsBuffer = []*dto.GlobalUserEvent{}
}

// there can be more events than asserters

// AssertHasEventsOrdered returns true if all the asserters are matched events in order of asserters
func (p *TestEventAccumulator) AssertHasEventsOrdered(asserters []func(e *dto.GlobalUserEvent) bool) bool {
	j := 0 // both second pointer and num of success comparisons

	for _, e := range p.eventsBuffer {
		if j >= len(asserters) { // bound check
			break
		}

		if asserters[j](e) {
			j++
		}
	}

	return j == len(asserters)
}

// AssertHasEventsUnordered returns true if all the asserters are matched events in any order
func (p *TestEventAccumulator) AssertHasEventsUnordered(asserters []func(e *dto.GlobalUserEvent) bool) bool {
	assertersCopy := make([]func(e *dto.GlobalUserEvent) bool, len(asserters))
	copy(assertersCopy, asserters)

	for _, e := range p.eventsBuffer {
		for j, c := range assertersCopy {
			if c(e) {
				assertersCopy = slices.Delete(assertersCopy, j, j+1)
				break // inner loop
			}
		}
	}

	return len(assertersCopy) == 0
}

func (p *TestEventAccumulator) AwaitForBufferContainsSpecifiedEvents(duration time.Duration, ordered bool, comparators []func(e *dto.GlobalUserEvent) bool) error {
	du := p.cfg.RabbitMQ.CheckAreEventsProcessedInterval

	startTime := time.Now()

	for {
		currTime := time.Now()
		if startTime.Add(duration).Before(currTime) {
			return fmt.Errorf("timeout error, there no specified events in %v", duration)
		}

		p.lgr.Info("Checking condition, the buffer is")
		if p.cfg.RabbitMQ.DumpTestAccumulator {
			spew.Dump(p.eventsBuffer)
		}

		fv := func() bool {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("panic occured: ", r)
				}
			}()
			if ordered {
				if p.AssertHasEventsOrdered(comparators) {
					p.lgr.Info("Buffer contains the specified events, exiting")
					return true
				}
			} else {
				if p.AssertHasEventsUnordered(comparators) {
					p.lgr.Info("Buffer contains the specified events, exiting")
					return true
				}
			}

			return false
		}()
		if fv {
			return nil // success exit
		}

		time.Sleep(du)
	}

}

type TestEventListener func(*amqp.Delivery) error

func CreateTestEventListener(service *TestEventAccumulator, lgr *logger.LoggerWrapper) TestEventListener {
	tr := otel.Tracer("amqp/listener")

	return func(msg *amqp.Delivery) error {
		ctx := rabbitmq.ExtractAMQPHeaders(context.Background(), msg.Headers)
		ctx, span := tr.Start(ctx, "test.event.listener")
		defer span.End()

		bytesData := msg.Body
		strData := string(bytesData)
		lgr.DebugContext(ctx, "Received", "data", strData)

		var bindTo = new(dto.GlobalUserEvent)
		err := json.Unmarshal(msg.Body, bindTo)
		if err != nil {
			lgr.ErrorContext(ctx, "Unable to unmarshall test event", "err", err)
			return err
		}

		service.OnEvent(ctx, bindTo)

		return nil
	}
}
