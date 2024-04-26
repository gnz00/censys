package processing

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

type ScanStore interface {
	SaveScan(ctx context.Context, message Message) error
}

type ScanRow struct {
	IpAddress string
	Port      uint32
	Service   string
	Timestamp time.Time
	Response  string
}

type DuckDbStore struct {
	conn *sql.DB // not optimal, but were running in memory and duckdb tables are per connection in memory so we have to reuse this connection
}

// create the enum types and scans table
func (db *DuckDbStore) init(ctx context.Context) error {
	_, err := db.conn.ExecContext(ctx, `CREATE TYPE SERVICE_TYPE AS ENUM ('SSH', 'HTTP', 'DNS');`)
	if err != nil {
		return err
	}

	_, err = db.conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS scans (
		ip_address STRING,
		port INTEGER,
		service SERVICE_TYPE,
		timestamp TIMESTAMP,
		response STRING,
		PRIMARY KEY (ip_address, port, service)
	);`)
	if err != nil {
		return err
	}

	return nil
}

func (db *DuckDbStore) SaveScan(ctx context.Context, message Message) error {
	_, err := db.conn.ExecContext(ctx, `
			INSERT INTO scans (ip_address, port, service, timestamp, response)
			VALUES (?, ?, ?, to_timestamp(?), ?)
			ON CONFLICT (ip_address, port, service)
			DO UPDATE 
			SET
				timestamp = excluded.timestamp,
				response = excluded.response
			WHERE scans.timestamp < excluded.timestamp;
		`, message.Ip, message.Port, message.Service, message.Timestamp, message.Response)

	return err
}

// prints the table to stdout
func (db *DuckDbStore) PrintTable(ctx context.Context) error {
	res, err := db.conn.QueryContext(ctx, `SELECT ip_address, port, service, timestamp, response FROM scans`)
	if err != nil {
		return err
	}
	defer res.Close()

	cols, err := res.Columns()
	if err != nil {
		return err
	}

	println("--- Scans Table ---")
	println(strings.Join(cols, ","))

	for res.Next() {
		row := ScanRow{}
		err = res.Scan(&row.IpAddress, &row.Port, &row.Service, &row.Timestamp, &row.Response)
		if err != nil {
			return err
		}

		println(fmt.Sprintf("%s,%d,%s,%d,%s", row.IpAddress, row.Port, row.Service, row.Timestamp.Unix(), row.Response))
	}

	return nil
}

func NewDuckDbStore() (*DuckDbStore, error) {
	conn, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}

	store := DuckDbStore{
		conn: conn,
	}

	store.init(context.Background())

	return &store, nil
}
