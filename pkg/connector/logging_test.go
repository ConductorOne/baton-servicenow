package connector

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func newBufferedLogCtx(buf *bytes.Buffer) context.Context {
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(buf), zapcore.DebugLevel)
	return ctxzap.ToContext(context.Background(), zap.New(core))
}

func TestShouldLogSample(t *testing.T) {
	tests := []struct {
		n    uint64
		want bool
	}{
		{0, true},
		{1, true},
		{2, false},
		{9, false},
		{10, true},
		{11, false},
		{99, false},
		{100, true},
		{101, false},
		{999, false},
		{1000, true},
		{1001, false},
		{1999, false},
		{2000, true},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, shouldLogSample(tt.n), "n=%d", tt.n)
	}
}

func TestSampledDebug_LogsOnlyOnSampledOccurrences(t *testing.T) {
	var buf bytes.Buffer
	ctx := newBufferedLogCtx(&buf)

	var s sampledDebug
	for i := 0; i < 12; i++ {
		s.log(ctx, "skipped row")
	}

	logs := buf.String()
	require.Equal(t, 2, strings.Count(logs, "skipped row"), "expected occurrences 1 and 10 out of 12")
	require.Contains(t, logs, `"total_occurrences":1}`)
	require.Contains(t, logs, `"total_occurrences":10}`)
	require.Equal(t, 2, strings.Count(logs, `"level":"debug"`))
}

// A noisy table must not push another table's first occurrence into a sampling gap.
func TestPerKeySampledDebug_NoisyKeyDoesNotStarveNewKey(t *testing.T) {
	var buf bytes.Buffer
	ctx := newBufferedLogCtx(&buf)

	var p perKeySampledDebug
	for i := 0; i < 400; i++ {
		p.log(ctx, "sys_user_has_role", "skipped row", zap.String("table", "sys_user_has_role"))
	}
	buf.Reset()

	p.log(ctx, "sys_group_has_role", "skipped row", zap.String("table", "sys_group_has_role"))

	logs := buf.String()
	require.Equal(t, 1, strings.Count(logs, "skipped row"))
	require.Contains(t, logs, `"table":"sys_group_has_role"`)
	require.Contains(t, logs, `"total_occurrences":1}`)
}
