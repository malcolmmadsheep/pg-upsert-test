package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var defaultMaxConns = 200

func NewPoolConfig(maxConns int32, connStr string) (*pgxpool.Config, error) {
	const defaultMinConns = int32(0)
	const defaultMaxConnLifetime = time.Hour
	const defaultMaxConnIdleTime = time.Minute * 30
	const defaultHealthCheckPeriod = time.Minute
	const defaultConnectTimeout = time.Second * 5

	dbConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}

	dbConfig.MaxConns = int32(maxConns)
	dbConfig.MinConns = defaultMinConns
	dbConfig.MaxConnLifetime = defaultMaxConnLifetime
	dbConfig.MaxConnIdleTime = defaultMaxConnIdleTime
	dbConfig.HealthCheckPeriod = defaultHealthCheckPeriod
	dbConfig.ConnConfig.ConnectTimeout = defaultConnectTimeout

	dbConfig.BeforeAcquire = func(ctx context.Context, c *pgx.Conn) bool {
		// log.Println("Before acquiring the connection pool to the database!!")
		return true
	}

	dbConfig.AfterRelease = func(c *pgx.Conn) bool {
		// log.Println("After releasing the connection pool to the database!!")
		return true
	}

	dbConfig.BeforeClose = func(c *pgx.Conn) {
		log.Println("Closed the connection pool to the database!!")
	}

	return dbConfig, nil
}

func main() {
	dbConnStr := os.Args[1]
	maxConns := int32(defaultMaxConns)

	if len(os.Args) > 2 {
		maxConnsParsed, err := strconv.ParseInt(os.Args[2], 10, 0)
		if err != nil {
			log.Fatalf("failed to parse max conns (%s): %v", os.Args[2], err)
		}

		maxConns = int32(maxConnsParsed)
	}

	poolConfig, err := NewPoolConfig(maxConns, dbConnStr)
	if err != nil {
		log.Fatalf("failed to create new pool config: %v", err)
	}

	connPool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		log.Fatalf("Error while creating connection to the database: %v", err)
	}
	defer func() {
		fmt.Println("closing conn pool")
		connPool.Close()
	}()

	connection, err := connPool.Acquire(context.Background())
	if err != nil {
		log.Fatal("Error while acquiring connection from the database pool!!")
	}
	defer connection.Release()

	if err := connection.Ping(context.Background()); err != nil {
		log.Fatalf("Could not ping database: %v", err)
	}

	if err := initTable(connPool); err != nil {
		log.Fatalf("failed to init table: %v", err)
	}

	content, err := os.ReadFile("uuids.txt")
	if err != nil {
		log.Fatalf("failed to read file: %v", err)
	}

	uuids := strings.Split(string(content), "\n")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		simulateUpserts(ctx.Done(), int(maxConns), uuids, connPool)
	}()

	<-ctx.Done()
}

func initTable(db *pgxpool.Pool) error {
	_, err := db.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS subscriptions (id TEXT PRIMARY KEY, count BIGINT);")

	return err
}

const query = `INSERT INTO subscriptions (id, count)
VALUES ($1, $2)
ON CONFLICT (id) DO UPDATE SET count = subscriptions.count + 1;
`

func simulateUpserts(done <-chan struct{}, workersCount int, uuids []string, db *pgxpool.Pool) {
	sem := make(chan struct{}, workersCount)
	previous := int64(0)
	count := int64(0)
	l := int64(len(uuids))

	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Second):
				current := atomic.LoadInt64(&count)

				delta := current - previous
				previous = current

				fmt.Printf("ops/s: %d; conns: %d;\n", delta, db.Stat().AcquiredConns())
			}

		}
	}()

	for {
		// <-time.After(100 * time.Millisecond)

		sem <- struct{}{}

		go func() {
			defer func() {
				<-sem
			}()

			_, err := db.Exec(context.Background(), query, uuids[atomic.LoadInt64(&count)%l], 0)
			if err != nil {
				fmt.Printf("failed to upsert: %v\n", err)
			}
			atomic.AddInt64(&count, 1)
		}()
	}
}
