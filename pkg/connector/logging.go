package connector

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type sampledDebug struct {
	n atomic.Uint64
}

func (s *sampledDebug) log(ctx context.Context, msg string, fields ...zap.Field) {
	n := s.n.Add(1)
	if !shouldLogSample(n) {
		return
	}
	ctxzap.Extract(ctx).Debug(msg, append(fields, zap.Uint64("total_occurrences", n))...)
}

type perKeySampledDebug struct {
	m sync.Map // key -> *sampledDebug
}

func (p *perKeySampledDebug) log(ctx context.Context, key, msg string, fields ...zap.Field) {
	actual, _ := p.m.LoadOrStore(key, &sampledDebug{})
	actual.(*sampledDebug).log(ctx, msg, fields...)
}

// shouldLogSample reports whether the nth occurrence should be logged.
func shouldLogSample(n uint64) bool {
	switch {
	case n <= 1, n == 10, n == 100:
		return true
	default:
		return n%1000 == 0
	}
}
