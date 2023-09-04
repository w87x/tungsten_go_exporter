package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type DatabaseNode struct {
	Name      string `json:"-"`
	QEDbStats struct {
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
		StatsInfo struct {
		} `json:"stats_info"`
		Errors struct {
			ReadTableFails        int `json:"read_table_fails"`
			WriteBatchColumnFails int `json:"write_batch_column_fails"`
			WriteTablespaceFails  int `json:"write_tablespace_fails"`
			ReadColumnFails       int `json:"read_column_fails"`
			ReadTablespaceFails   int `json:"read_tablespace_fails"`
			WriteTableFails       int `json:"write_table_fails"`
			WriteColumnFails      int `json:"write_column_fails"`
		} `json:"errors"`
	} `json:"QEDbStats"`
	DatabaseUsageInfo struct {
		DatabaseUsage []struct {
			DiskSpaceAvailable1K int `json:"disk_space_available_1k"`
			AnalyticsDbSize1K    int `json:"analytics_db_size_1k"`
			DiskSpaceUsed1K      int `json:"disk_space_used_1k"`
		} `json:"database_usage"`
	} `json:"DatabaseUsageInfo"`
	CassandraStatusData struct {
		CassandraCompactionTask struct {
			PendingCompactionTasks int `json:"pending_compaction_tasks"`
		} `json:"cassandra_compaction_task"`
	} `json:"CassandraStatusData"`
}

func (v *DatabaseNode) ToMap() NamedMap {
	r := make(map[string]float64)
	r["requests_one_minute_rate"] = float64(v.QEDbStats.CqlStats.RequestsOneMinuteRate)
	r["exceeded_pending_requests_water_mark"] = float64(v.QEDbStats.CqlStats.Stats.ExceededPendingRequestsWaterMark)
	r["available_connections"] = float64(v.QEDbStats.CqlStats.Stats.AvailableConnections)
	r["exceeded_write_bytes_water_mark"] = float64(v.QEDbStats.CqlStats.Stats.ExceededWriteBytesWaterMark)
	r["total_connections"] = float64(v.QEDbStats.CqlStats.Stats.TotalConnections)
	r["connection_timeouts"] = float64(v.QEDbStats.CqlStats.Errors.ConnectionTimeouts)
	r["pending_request_timeouts"] = float64(v.QEDbStats.CqlStats.Errors.PendingRequestTimeouts)
	r["request_timeouts"] = float64(v.QEDbStats.CqlStats.Errors.RequestTimeouts)
	r["read_table_fails"] = float64(v.QEDbStats.Errors.ReadTableFails)
	r["write_batch_column_fails"] = float64(v.QEDbStats.Errors.WriteBatchColumnFails)
	r["write_tablespace_fails"] = float64(v.QEDbStats.Errors.WriteTablespaceFails)
	r["read_column_fails"] = float64(v.QEDbStats.Errors.ReadColumnFails)
	r["read_tablespace_fails"] = float64(v.QEDbStats.Errors.ReadTablespaceFails)
	r["write_table_fails"] = float64(v.QEDbStats.Errors.WriteTableFails)
	r["write_column_fails"] = float64(v.QEDbStats.Errors.WriteColumnFails)
	r["pending_compaction_tasks"] = float64(v.CassandraStatusData.CassandraCompactionTask.PendingCompactionTasks)
	for i := 0; i < len(v.DatabaseUsageInfo.DatabaseUsage); i++ {
		r[fmt.Sprintf("disk_space_available_1k.%v", i)] = float64(v.DatabaseUsageInfo.DatabaseUsage[i].DiskSpaceAvailable1K)
		r[fmt.Sprintf("analytics_db_size_1k.%v", i)] = float64(v.DatabaseUsageInfo.DatabaseUsage[i].AnalyticsDbSize1K)
		r[fmt.Sprintf("disk_space_used_1k.%v", i)] = float64(v.DatabaseUsageInfo.DatabaseUsage[i].DiskSpaceUsed1K)
	}

	return NamedMap{Map: r, Name: v.Name}
}
func get_db_nodes() ([]NamedMap, error) {
	hrefs, err := get_href_list("database-node")
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
			v := DatabaseNode{}
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
	log.Debug().Msg("database nodes downloaded")

	return result, nil
}

type TungstenDbNodeCollector struct {
}

func (e *TungstenDbNodeCollector) Describe(ch chan<- *prometheus.Desc) {
	metrics := []string{
		"G#requests_one_minute_rate",
		"G#exceeded_pending_requests_water_mark",
		"G#available_connections", "G#exceeded_write_bytes_water_mark", "G#total_connections", "connection_timeouts",
		"pending_request_timeouts", "request_timeouts", "read_table_fails", "write_batch_column_fails",
		"write_tablespace_fails", "read_column_fails", "read_tablespace_fails", "write_table_fails", "write_column_fails", "G#disk_space_available_1k", "G#analytics_db_size_1k", "G#disk_space_used_1k"}
	for _, metric := range metrics {
		_, n := get_metric_type(metric)
		if strings.Contains(n, "_1k") {
			db_metric_desc[metric] = prometheus.NewDesc(prometheus.BuildFQName("tf", "db_node", n), "", []string{"vrouter", "disk"}, nil)
		} else {
			db_metric_desc[metric] = prometheus.NewDesc(prometheus.BuildFQName("tf", "db_node", n), "", []string{"vrouter"}, nil)
		}
	}
}

var db_metric_desc = make(map[string]*prometheus.Desc)

func (e *TungstenDbNodeCollector) Collect(ch chan<- prometheus.Metric) {
	for _, v := range db_nodes_maps {
		for k, m := range db_metric_desc {
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
