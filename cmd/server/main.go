package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"gdisw/metric-store/internal/grpcserver"
	"gdisw/metric-store/internal/otelpipe"
	"gdisw/metric-store/internal/store"
	chstore 	"gdisw/metric-store/internal/store/clickhouse"
)

var version = "dev"

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envIntOr(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

var (
	listenAddr            = flag.String("listenAddr", envOr("LISTEN_ADDR", "localhost:4317"), "gRPC listen address")
	maxReceiveMessageSize = flag.Int("maxReceiveMessageSize", envIntOr("MAX_RECV_MSG_SIZE", 16777216), "max receive message size in bytes")
	storeKind             = flag.String("store", envOr("STORE", "memory"), "metrics backend: memory or clickhouse")
	chAddr                = flag.String("clickhouse.addr", envOr("CLICKHOUSE_ADDR", "localhost:9000"), "ClickHouse native address host:port")
	chDatabase            = flag.String("clickhouse.database", envOr("CLICKHOUSE_DATABASE", "default"), "ClickHouse database")
	chUsername            = flag.String("clickhouse.username", envOr("CLICKHOUSE_USERNAME", "default"), "ClickHouse username")
	chPasswordFile        = flag.String("clickhouse.password-file", envOr("CLICKHOUSE_PASSWORD_FILE", ""), "path to file containing ClickHouse password; empty for no password")
)

func main() {
	flag.Parse()

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

	slog.SetDefault(grpcserver.Logger())
	slog.Info("starting OTLP metrics server", slog.String("version", version))

	chPassword, err := clickHousePassword()
	if err != nil {
		log.Fatal(err)
	}

	st, err := openStore(ctx, chPassword)
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

func clickHousePassword() (string, error) {
	if *chPasswordFile != "" {
		b, err := os.ReadFile(*chPasswordFile)
		if err != nil {
			return "", fmt.Errorf("read clickhouse password file: %w", err)
		}
		return strings.TrimRight(string(b), "\r\n"), nil
	}
	if v, ok := os.LookupEnv("CLICKHOUSE_PASSWORD"); ok && v != "" {
		return v, nil
	}
	return "", nil
}

func openStore(ctx context.Context, chPassword string) (store.MetricsStore, error) {
	switch *storeKind {
	case "memory":
		return store.NewMemory(), nil
	case "clickhouse":
		return chstore.Open(ctx, chstore.Config{
			Addr:     *chAddr,
			Database: *chDatabase,
			Username: *chUsername,
			Password: chPassword,
		})
	default:
		return nil, errors.New("unknown --store (use memory or clickhouse)")
	}
}
