package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

var (
	configuration   Config
	authToken       string
	authTokenExpiry time.Time
)
var vrouter_maps = []NamedMap{}

// var vminterfaces = []VirtualMachineInterface{}
var db_nodes_maps = []NamedMap{}
var adb_nodes_maps = []NamedMap{}
var cdb_nodes_maps = []NamedMap{}
var bgp_peer_maps = []NamedMap{}

type NamedMap struct {
	Map    map[string]float64
	Name   string
	Labels []string
}

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "02.01.06 15:04:05Z07"})
	log.Info().Msg("tungsten exporter init")
	config_file := flag.String("config", "./config.yaml", "Specify configuration file")
	flag.Parse()
	yamlFile, err := os.ReadFile(*config_file)
	if err != nil {
		log.Error().Msgf("configuration loading error #%v ", err)
	}
	conf := &Config{}
	err = yaml.Unmarshal(yamlFile, conf)
	if err != nil {
		log.Fatal().Msgf("configuration unmarshal error: %v", err)
	}
	configuration = *conf
	switch strings.ToLower(configuration.LogLevel) {
	case "debug":
		log.Logger = log.Logger.Level(zerolog.DebugLevel)
	case "info":
		log.Logger = log.Logger.Level(zerolog.InfoLevel)
	case "warn":
		log.Logger = log.Logger.Level(zerolog.WarnLevel)
	case "error":
		log.Logger = log.Logger.Level(zerolog.ErrorLevel)
	default:
		log.Logger = log.Logger.Level(zerolog.InfoLevel)
	}
	log.Info().Msg("tungsten exporter config loaded")
}
func main() {
	if configuration.Scrape.Interval == 0 {
		configuration.Scrape.Interval = 30
	}
	prometheus.Register(&TungstenVrouterCollector{})
	prometheus.Register(&TungstenDbNodeCollector{})
	prometheus.Register(&TungstenAnalyticsDbNodeCollector{})
	prometheus.Register(&TungstenBgpPeerCollector{})
	prometheus.Register(&TungstenConfigDbNodeCollector{})
	prometheus.Unregister(collectors.NewGoCollector())
	go func() {
		for {
			var err error
			go func() {
				vrouter_maps, err = get_vrouters()
				if err != nil {
					log.Error().Err(err).Msg("")
				} else {
					log.Info().Msg("vrouters collected")
				}
			}()
			go func() {
				db_nodes_maps, err = get_db_nodes()
				if err != nil {
					log.Error().Err(err).Msg("")
				} else {
					log.Info().Msg("db nodes collected")
				}
			}()
			go func() {
				adb_nodes_maps, err = get_adb_nodes()
				if err != nil {
					log.Error().Err(err).Msg("")
				} else {
					log.Info().Msg("analytics nodes collected")
				}
			}()
			go func() {
				cdb_nodes_maps, err = get_cdb_nodes()
				if err != nil {
					log.Error().Err(err).Msg("")
				} else {
					log.Info().Msg("config database nodes collected")
				}
			}()
			go func() {
				bgp_peer_maps, err = get_bgp_peers()
				if err != nil {
					log.Error().Err(err).Msg("")
				} else {
					log.Info().Msg("bgp peers collected")
				}
			}()

			time.Sleep(time.Duration(configuration.Scrape.Interval) * time.Second)
		}
	}()
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(configuration.Web.Listen, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
}

func get_href_list(uve_type string) ([]named_href, error) {
	b, err := tungsten_get_req(fmt.Sprintf("/analytics/uves/%vs", uve_type))
	if err != nil {
		return nil, err
	}
	href_list := make([]named_href, 0)
	err = json.Unmarshal(*b, &href_list)
	return href_list, err
}

/*
	func get_vm_interfaces() ([]VirtualMachineInterface, error) {
		hrefs, err := get_href_list("virtual-machine-interface")
		if err != nil {
			return nil, err
		}
		var wg sync.WaitGroup
		result := make([]VirtualMachineInterface, 0)

		var download_threads = 0

		for _, href := range hrefs {
			for download_threads > configuration.Scrape.Threads {
				time.Sleep(10 * time.Millisecond)
			}
			wg.Add(1)
			download_threads++

			go func(h string) {
				log.Debug().Msgf("downloading %v", h)

				b, err := tungsten_get_req(h)
				if err != nil {
					log.Warn().Err(err).Msgf("error downloading %v", h)
				}
				v := VirtualMachineInterface{}
				err = json.Unmarshal(*b, &v)
				if err != nil {
					log.Warn().Err(err).Msgf("error parsing %v", h)
				}
				result = append(result, v)
				download_threads--
				wg.Done()
			}(href.Href)
		}
		wg.Wait()
		log.Debug().Msg("vm interfaces downloaded")

		return result, nil
	}
*/

func auth() error {
	log.Debug().Msg("authentication")
	token_req := fmt.Sprintf(`{ "auth": { "identity": { "methods": ["password"], "password": { "user": { "name": "%v", "domain": { "id": "%v" }, "password": "%v" }  } }, "scope": { "project": { "name": "%v", "domain": { "id": "%v" } } } } }`, configuration.Keystone.User, configuration.Keystone.Domain, configuration.Keystone.Password, configuration.Keystone.Project, configuration.Keystone.ProjectDomain)
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: !configuration.Keystone.Tls}
	token_resp, err := http.Post(configuration.Keystone.Url, "application/json", bytes.NewBuffer([]byte(token_req)))

	if err != nil {
		return err
	}
	authToken = token_resp.Header.Get("X-Subject-Token")
	authTokenExpiry = time.Now().Add(2 * time.Minute)

	if authToken == "" {
		return errors.New("no auth token received from keystone")
	} else {
		return nil
	}
}
func tungsten_get_req(path string) (*[]byte, error) {
	if authToken == "" || authTokenExpiry.Before(time.Now()) {
		for retry := 0; retry < configuration.Scrape.Retries; retry++ {
			if auth_err := auth(); auth_err == nil {
				break
			} else {
				log.Warn().Err(auth_err).Msg("")
			}
		}
	}

	if authToken == "" {
		return nil, errors.New("authentication error")
	}
	if u, err := url.Parse(path); err != nil || !u.IsAbs() {
		if path != "" && !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		path = configuration.Scrape.Url + path
	}
	var r_err error
	var r []byte
	for retry := 0; retry < configuration.Scrape.Retries; retry++ {
		ctx, cncl := context.WithTimeout(context.Background(), time.Second*time.Duration(configuration.Scrape.Timeout))
		defer cncl()
		req, err := http.NewRequestWithContext(ctx, "GET", path, &bytes.Buffer{})
		if err != nil {
			r_err = err
			log.Warn().Err(err).Msg("")
			continue
		}
		req.Header.Add("X-Auth-Token", authToken)
		req.Header.Add("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			r_err = err
			log.Warn().Err(err).Msg("")
			continue
		} else {
			r_err = nil
			r, r_err = io.ReadAll(resp.Body)
			log.Debug().Msgf("tungsten request to path %v", req.URL)
			break
		}
	}
	return &r, r_err
}

func get_metric_type(name string) (prometheus.ValueType, string) {
	s := strings.Split(name, "#")
	switch strings.ToUpper(s[0]) {
	case "G":
		return prometheus.GaugeValue, s[1]
	case "C":
		return prometheus.CounterValue, s[1]
	case "U":
		return prometheus.UntypedValue, s[1]
	default:
		return prometheus.CounterValue, name
	}

}
