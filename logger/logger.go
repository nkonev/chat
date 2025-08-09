package logger

import (
	"context"
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/config"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
	"strings"
	"time"
)

const LogFieldTraceId = "trace_id"

func GetTraceId(ctx context.Context) string {
	sc := trace.SpanFromContext(ctx).SpanContext()
	tr := sc.TraceID()
	return tr.String()
}

type TracingContextHandler struct {
	slog.Handler
}

func (h *TracingContextHandler) Handle(ctx context.Context, r slog.Record) error {
	traceId := GetTraceId(ctx)
	if traceId != "" {
		r.AddAttrs(slog.String(LogFieldTraceId, traceId))
	}

	return h.Handler.Handle(ctx, r)
}

type LoggerWrapper struct {
	*slog.Logger
}

func NewBaseLogger(w io.Writer, cfg *config.AppConfig) *slog.Logger {
	var baseLogger *slog.Logger

	replaceFunc := func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == "msg" {
			return slog.Attr{
				Key:   "message",
				Value: a.Value,
			}
		} else if a.Key == "time" {
			utcTime := time.Now().UTC()
			utcFormattedTime := utcTime.Format("2006-01-02T15:04:05.000000000Z")
			return slog.Attr{
				Key:   "@timestamp",
				Value: slog.AnyValue(utcFormattedTime),
			}
		} else if a.Key == "level" {
			return slog.Attr{
				Key:   "level",
				Value: slog.StringValue(strings.ToLower(a.Value.String())),
			}
		} else if a.Key == "file" || a.Key == "source" {
			return slog.Attr{
				Key:   "caller",
				Value: a.Value,
			}
		} else {
			return a
		}
	}

	bh := &slog.HandlerOptions{
		Level:       cfg.Logger.GetLevel(),
		ReplaceAttr: replaceFunc,
		AddSource:   true,
	}
	commonAttrs := []slog.Attr{slog.String("service", app.TRACE_RESOURCE)}
	if cfg.Logger.Json {
		h := &TracingContextHandler{slog.NewJSONHandler(w, bh).WithAttrs(commonAttrs)}
		baseLogger = slog.New(h)
	} else {
		h := &TracingContextHandler{slog.NewTextHandler(w, bh).WithAttrs(commonAttrs)}
		baseLogger = slog.New(h)
	}

	return baseLogger
}

func NewLogger(base *slog.Logger) *LoggerWrapper {
	return &LoggerWrapper{
		Logger: base,
	}
}

func (lw *LoggerWrapper) WithTrace0(ctx context.Context) *slog.Logger {
	return lw.Logger.With(LogFieldTraceId, GetTraceId(ctx))
}
