package main

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type AnalyticsNode struct {
	Name             string `json:"-"`
	CollectorDbStats struct {
		CqlStats struct {
			RequestsOneMinuteRate float64 `json:"requests_one_minute_rate"`
			Stats                 struct {
				ExceededPendingRequestsWaterMark int `json:"exceeded_pending_requests_water_mark"`
				AvailableConnections             int `json:"available_connections"`
				ExceededWriteBytesWaterMark      int `json:"exceeded_write_bytes_water_mark"`
				TotalConnections                 int `json:"total_connections"`
			} `json:"stats"`
			Errors struct {
				ConnectionTimeouts     int `json:"connection_timeouts"`
				PendingRequestTimeouts int `json:"pending_request_timeouts"`
				RequestTimeouts        int `json:"request_timeouts"`
			} `json:"errors"`
		} `json:"cql_stats"`
		Errors struct {
			ReadTableFails        int `json:"read_table_fails"`
			WriteBatchColumnFails int `json:"write_batch_column_fails"`
			WriteTablespaceFails  int `json:"write_tablespace_fails"`
			ReadColumnFails       int `json:"read_column_fails"`
			ReadTablespaceFails   int `json:"read_tablespace_fails"`
			WriteTableFails       int `json:"write_table_fails"`
			WriteColumnFails      int `json:"write_column_fails"`
		} `json:"errors"`
		SessionTableStats struct {
			NumMessages         int     `json:"num_messages"`
			SessionsPerDbRecord float64 `json:"sessions_per_db_record"`
			WritesPerMessage    float64 `json:"writes_per_message"`
			JSONSizePerWrite    float64 `json:"json_size_per_write"`
		} `json:"session_table_stats"`
	} `json:"CollectorDbStats"`
	CollectorState struct {
		RxSocketStats struct {
			BlockedCount int     `json:"blocked_count"`
			Errors       int     `json:"errors"`
			Calls        int     `json:"calls"`
			Bytes        int     `json:"bytes"`
			AverageBytes float64 `json:"average_bytes"`
		} `json:"rx_socket_stats"`
		TxSocketStats struct {
			BlockedCount int     `json:"blocked_count"`
			Errors       int     `json:"errors"`
			Calls        int     `json:"calls"`
			Bytes        int     `json:"bytes"`
			AverageBytes float64 `json:"average_bytes"`
		} `json:"tx_socket_stats"`
	} `json:"CollectorState"`
}

func (v *AnalyticsNode) ToMap() NamedMap {
	r := make(map[string]float64)
	r["requests_one_minute_rate"] = float64(v.CollectorDbStats.CqlStats.RequestsOneMinuteRate)
	r["exceeded_pending_requests_water_mark"] = float64(v.CollectorDbStats.CqlStats.Stats.ExceededPendingRequestsWaterMark)
	r["available_connections"] = float64(v.CollectorDbStats.CqlStats.Stats.AvailableConnections)
	r["exceeded_write_bytes_water_mark"] = float64(v.CollectorDbStats.CqlStats.Stats.ExceededWriteBytesWaterMark)
	r["total_connections"] = float64(v.CollectorDbStats.CqlStats.Stats.TotalConnections)
	r["connection_timeouts"] = float64(v.CollectorDbStats.CqlStats.Errors.ConnectionTimeouts)
	r["pending_request_timeouts"] = float64(v.CollectorDbStats.CqlStats.Errors.PendingRequestTimeouts)
	r["request_timeouts"] = float64(v.CollectorDbStats.CqlStats.Errors.RequestTimeouts)
	r["read_table_fails"] = float64(v.CollectorDbStats.Errors.ReadTableFails)
	r["write_batch_column_fails"] = float64(v.CollectorDbStats.Errors.WriteBatchColumnFails)
	r["write_tablespace_fails"] = float64(v.CollectorDbStats.Errors.WriteTablespaceFails)
	r["read_column_fails"] = float64(v.CollectorDbStats.Errors.ReadColumnFails)
	r["read_tablespace_fails"] = float64(v.CollectorDbStats.Errors.ReadTablespaceFails)
	r["write_table_fails"] = float64(v.CollectorDbStats.Errors.WriteTableFails)
	r["write_column_fails"] = float64(v.CollectorDbStats.Errors.WriteColumnFails)

	//TODO! from SessionTableStats
	//TODO! from CollectorState
	return NamedMap{Map: r, Name: v.Name}
}

func get_adb_nodes() ([]NamedMap, error) {
	hrefs, err := get_href_list("analytics-node")
	if err != nil {
		return nil, err
	}
	log.Debug().Msgf("vrouters hrefs: %v", len(hrefs))
	var wg sync.WaitGroup
	result := make([]NamedMap, 0)

	var download_threads = 0

	for _, href := range hrefs {
		for download_threads > configuration.Scrape.Threads {
			time.Sleep(10 * time.Millisecond)
		}
		wg.Add(1)
		download_threads++

		go func(h named_href) {
			log.Debug().Msgf("downloading %v", h.Name)

			b, err := tungsten_get_req(h.Href)
			if err != nil {
				log.Warn().Err(err).Msgf("error downloading %v", h)
			}
			v := AnalyticsNode{}
			v.Name = h.Name
			f, _ := os.Create(v.Name)
			f.Write(*b)
			f.Sync()
			f.Close()
			err = json.Unmarshal(*b, &v)
			if err != nil {
				log.Warn().Err(err).Msgf("error parsing %v", h)
			}
			result = append(result, v.ToMap())
			download_threads--
			wg.Done()
		}(href)
	}
	wg.Wait()
	log.Debug().Msg("analytics nodes downloaded")

	return result, nil
}

type TungstenAnalyticsDbNodeCollector struct {
}

func (e *TungstenAnalyticsDbNodeCollector) Describe(ch chan<- *prometheus.Desc) {
	metrics := []string{
		"G#requests_one_minute_rate",
		"exceeded_pending_requests_water_mark",
		"G#available_connections", "exceeded_write_bytes_water_mark", "G#total_connections", "connection_timeouts",
		"pending_request_timeouts", "request_timeouts", "read_table_fails", "write_batch_column_fails",
		"write_tablespace_fails", "read_column_fails", "read_tablespace_fails", "write_table_fails", "write_column_fails", "G#disk_space_available_1k", "G#analytics_db_size_1k", "G#disk_space_used_1k"}
	for _, metric := range metrics {
		_, n := get_metric_type(metric)
		if strings.Contains(n, "_1k") {
			adb_metric_desc[metric] = prometheus.NewDesc(prometheus.BuildFQName("tf", "analytics_node", n), "", []string{"node", "disk"}, nil)
		} else {
			adb_metric_desc[metric] = prometheus.NewDesc(prometheus.BuildFQName("tf", "analytics_node", n), "", []string{"node"}, nil)
		}
	}
}

var adb_metric_desc = make(map[string]*prometheus.Desc)

func (e *TungstenAnalyticsDbNodeCollector) Collect(ch chan<- prometheus.Metric) {
	for _, v := range adb_nodes_maps {
		for k, m := range adb_metric_desc {
			t, n := get_metric_type(k)
			if strings.Contains(k, "_1k") {
				for key, val := range v.Map {
					if strings.Index(key, n) == 0 {
						disk := strings.Split(key, ".")[1]
						ch <- prometheus.MustNewConstMetric(m, t, val, v.Name, disk)
					}
				}
			} else {
				ch <- prometheus.MustNewConstMetric(m, t, v.Map[n], v.Name)
			}
		}
	}
}
