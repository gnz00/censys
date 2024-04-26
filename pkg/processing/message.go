package processing

import (
	"encoding/json"

	"github.com/censys/scan-takehome/pkg/scanning"
)

type Message struct {
	Ip        string
	Port      uint32
	Service   string
	Timestamp int64
	Response  string
}

// Normalizes the response from v1 and v2 into a single struct for persisting to the database
func (m *Message) UnmarshalJSON(data []byte) error {
	scan := scanning.Scan{}
	err := json.Unmarshal(data, &scan)
	if err != nil {
		return err
	}

	m.Ip = scan.Ip
	m.Port = scan.Port
	m.Service = scan.Service
	m.Timestamp = scan.Timestamp

	if scan.DataVersion == 1 {
		str, err := json.Marshal(scan.Data)
		if err != nil {
			return err
		}

		data := scanning.V1Data{}
		err = json.Unmarshal(str, &data)
		if err != nil {
			return err
		}
		m.Response = string(data.ResponseBytesUtf8)
	}

	if scan.DataVersion == 2 {
		str, err := json.Marshal(scan.Data)
		if err != nil {
			return err
		}

		data := scanning.V2Data{}
		err = json.Unmarshal(str, &data)
		if err != nil {
			return err
		}
		m.Response = string(data.ResponseStr)
	}

	return nil
}
