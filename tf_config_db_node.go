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

type ConfigDatabaseNode struct {
	Name              string `json:"-"`
	DatabaseUsageInfo struct {
		DatabaseUsage []struct {
			DiskSpaceAvailable1K int `json:"disk_space_available_1k"`
			ConfigDbSize1K       int `json:"config_db_size_1k"`
			DiskSpaceUsed1K      int `json:"disk_space_used_1k"`
		} `json:"database_usage"`
	} `json:"DatabaseUsageInfo"`
	CassandraStatusData struct {
		ThreadPoolStats []struct {
			Active         int    `json:"active"`
			PoolName       string `json:"pool_name"`
			AllTimeBlocked int    `json:"all_time_blocked"`
			Pending        int    `json:"pending"`
		} `json:"thread_pool_stats"`
		CassandraCompactionTask struct {
			PendingCompactionTasks int `json:"pending_compaction_tasks"`
		} `json:"cassandra_compaction_task"`
	} `json:"CassandraStatusData"`
}

func get_cdb_nodes() ([]NamedMap, error) {
	hrefs, err := get_href_list("config-database-node")
	if err != nil {
		return nil, err
	}
	log.Debug().Msgf("config database hrefs: %v", len(hrefs))
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
			v := ConfigDatabaseNode{}
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
	log.Debug().Msg("config database nodes downloaded")

	return result, nil
}
func (v *ConfigDatabaseNode) ToMap() NamedMap {
	r := make(map[string]float64)
	for _, pool := range v.CassandraStatusData.ThreadPoolStats {
		r[fmt.Sprintf("thread_pool_stats_active.%v", pool.PoolName)] = float64(pool.Active)
		r[fmt.Sprintf("thread_pool_stats_all_time_blocked.%v", pool.PoolName)] = float64(pool.AllTimeBlocked)
		r[fmt.Sprintf("thread_pool_stats_pending.%v", pool.PoolName)] = float64(pool.Pending)
	}
	r["pending_compaction_tasks"] = float64(v.CassandraStatusData.CassandraCompactionTask.PendingCompactionTasks)
	for i := 0; i < len(v.DatabaseUsageInfo.DatabaseUsage); i++ {
		r[fmt.Sprintf("disk_space_available_1k.%v", i)] = float64(v.DatabaseUsageInfo.DatabaseUsage[i].DiskSpaceAvailable1K)
		r[fmt.Sprintf("config_db_size_1k.%v", i)] = float64(v.DatabaseUsageInfo.DatabaseUsage[i].ConfigDbSize1K)
		r[fmt.Sprintf("disk_space_used_1k.%v", i)] = float64(v.DatabaseUsageInfo.DatabaseUsage[i].DiskSpaceUsed1K)
	}
	return NamedMap{Map: r, Name: v.Name}
}

type TungstenConfigDbNodeCollector struct {
}

func (e *TungstenConfigDbNodeCollector) Describe(ch chan<- *prometheus.Desc) {
	metrics := []string{"G#disk_space_available_1k", "G#config_db_size_1k", "G#disk_space_used_1k",
		"G#pending_compaction_tasks", "G#thread_pool_stats_active", "G#thread_pool_stats_all_time_blocked", "G#thread_pool_stats_pending"}
	for _, metric := range metrics {
		_, n := get_metric_type(metric)
		if strings.Contains(n, "_1k") {
			cdb_metric_desc[metric] = prometheus.NewDesc(prometheus.BuildFQName("tf", "db_node", n), "", []string{"node", "disk"}, nil)
		} else {
			if strings.Contains(n, "thread_pool_stats") {
				cdb_metric_desc[metric] = prometheus.NewDesc(prometheus.BuildFQName("tf", "db_node", n), "", []string{"node", "pool"}, nil)
			} else {
				cdb_metric_desc[metric] = prometheus.NewDesc(prometheus.BuildFQName("tf", "db_node", n), "", []string{"node"}, nil)
			}
		}

	}
}

var cdb_metric_desc = make(map[string]*prometheus.Desc)

func (e *TungstenConfigDbNodeCollector) Collect(ch chan<- prometheus.Metric) {
	for _, v := range cdb_nodes_maps {
		for k, m := range cdb_metric_desc {
			t, n := get_metric_type(k)
			if strings.Contains(k, "_1k") || strings.Contains(k, "thread_pool_stats") {
				for key, val := range v.Map {
					if strings.Index(key, n) == 0 {
						label := strings.Split(key, ".")[1]
						ch <- prometheus.MustNewConstMetric(m, t, val, v.Name, label)
					}
				}
			} else {
				ch <- prometheus.MustNewConstMetric(m, t, v.Map[n], v.Name)
			}
		}
	}
}
