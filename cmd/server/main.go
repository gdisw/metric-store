package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"dash0.com/otlp-log-processor-backend/internal/grpcserver"
	"dash0.com/otlp-log-processor-backend/internal/otelpipe"
	"dash0.com/otlp-log-processor-backend/internal/store"
	chstore "dash0.com/otlp-log-processor-backend/internal/store/clickhouse"
)

var (
	listenAddr            = flag.String("listenAddr", "localhost:4317", "gRPC listen address")
	maxReceiveMessageSize = flag.Int("maxReceiveMessageSize", 16777216, "max receive message size in bytes")
	storeKind             = flag.String("store", "memory", "metrics backend: memory or clickhouse")
	chAddr                = flag.String("clickhouse.addr", "localhost:9000", "ClickHouse native address host:port")
	chDatabase            = flag.String("clickhouse.database", "default", "ClickHouse database")
	chUsername            = flag.String("clickhouse.username", "default", "ClickHouse username")
	chPassword            = flag.String("clickhouse.password", "", "ClickHouse password")
)

func main() {
	slog.SetDefault(grpcserver.Logger())
	slog.Info("starting OTLP metrics server")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	otelShutdown, err := otelpipe.SetupOTelSDK(ctx)
	if err != nil {
		log.Fatalf("otel setup: %v", err)
	}
	defer func() {
		if e := otelShutdown(context.Background()); e != nil {
			slog.Error("otel shutdown", slog.Any("err", e))
		}
	}()

	flag.Parse()

	st, err := openStore(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if err := st.CreateTables(ctx); err != nil {
		_ = st.Close()
		log.Fatalf("create tables: %v", err)
	}

	err = grpcserver.Run(ctx, grpcserver.RunConfig{
		ListenAddr:     *listenAddr,
		MaxRecvMsgSize: *maxReceiveMessageSize,
	}, st)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func openStore(ctx context.Context) (store.MetricsStore, error) {
	switch *storeKind {
	case "memory":
		return store.NewMemory(), nil
	case "clickhouse":
		return chstore.Open(ctx, chstore.Config{
			Addr:     *chAddr,
			Database: *chDatabase,
			Username: *chUsername,
			Password: *chPassword,
		})
	default:
		return nil, errors.New("unknown --store (use memory or clickhouse)")
	}
}
