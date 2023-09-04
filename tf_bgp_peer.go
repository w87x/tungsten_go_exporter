package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type BgpPeer struct {
	Name          string `json:"-"`
	PeerStatsData struct {
		RawTxUpdateStats struct {
			Unreach  int `json:"unreach"`
			Total    int `json:"total"`
			Reach    int `json:"reach"`
			EndOfRib int `json:"end_of_rib"`
		} `json:"raw_tx_update_stats"`
		RawRxUpdateStats struct {
			Unreach  int `json:"unreach"`
			Total    int `json:"total"`
			Reach    int `json:"reach"`
			EndOfRib int `json:"end_of_rib"`
		} `json:"raw_rx_update_stats"`
	} `json:"PeerStatsData"`
	PeerFlapData struct {
		FlapInfo struct {
			FlapCount int   `json:"flap_count"`
			FlapTime  int64 `json:"flap_time"`
		} `json:"flap_info"`
	} `json:"PeerFlapData"`
	BgpPeerInfoData struct {
		LocalAsn      int    `json:"local_asn"`
		PeerID        int64  `json:"peer_id"`
		RouteOrigin   string `json:"route_origin"`
		PeerAddress   string `json:"peer_address"`
		PeerAsn       int    `json:"peer_asn"`
		PeerStatsInfo struct {
			RxProtoStats struct {
				Notification int `json:"notification"`
				Update       int `json:"update"`
				Close        int `json:"close"`
				Total        int `json:"total"`
				Open         int `json:"open"`
				Keepalive    int `json:"keepalive"`
			} `json:"rx_proto_stats"`
			RxUpdateStats struct {
				Unreach  int `json:"unreach"`
				Total    int `json:"total"`
				Reach    int `json:"reach"`
				EndOfRib int `json:"end_of_rib"`
			} `json:"rx_update_stats"`
			TxProtoStats struct {
				Notification int `json:"notification"`
				Update       int `json:"update"`
				Close        int `json:"close"`
				Total        int `json:"total"`
				Open         int `json:"open"`
				Keepalive    int `json:"keepalive"`
			} `json:"tx_proto_stats"`
			TxUpdateStats struct {
				Unreach  int `json:"unreach"`
				Total    int `json:"total"`
				Reach    int `json:"reach"`
				EndOfRib int `json:"end_of_rib"`
			} `json:"tx_update_stats"`
			RxRouteStats struct {
				PrimaryPathCount int `json:"primary_path_count"`
				TotalPathCount   int `json:"total_path_count"`
			} `json:"rx_route_stats"`
			RxErrorStats struct {
				Inet6ErrorStats struct {
					BadInet6XMLTokenCount int `json:"bad_inet6_xml_token_count"`
					BadInet6AfiSafiCount  int `json:"bad_inet6_afi_safi_count"`
					BadInet6NexthopCount  int `json:"bad_inet6_nexthop_count"`
					BadInet6PrefixCount   int `json:"bad_inet6_prefix_count"`
				} `json:"inet6_error_stats"`
			} `json:"rx_error_stats"`
		} `json:"peer_stats_info"`
		RouterType string `json:"router_type"`
	} `json:"BgpPeerInfoData"`
}

func get_bgp_peers() ([]NamedMap, error) {
	hrefs, err := get_href_list("bgp-peer")
	if err != nil {
		return nil, err
	}
	log.Debug().Msgf("bgp-peers hrefs: %v", len(hrefs))
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
			v := BgpPeer{}
			v.Name = h.Name
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
	log.Debug().Msg("vrouters downloaded")

	return result, nil
}

type TungstenBgpPeerCollector struct {
}

func (v *BgpPeer) ToMap() NamedMap {
	r := make(map[string]float64)
	l := make([]string, 0) //Local ASN, Peer ASN,
	r["tx_update_unreach"] = float64(v.PeerStatsData.RawTxUpdateStats.Unreach)
	r["rx_update_unreach"] = float64(v.PeerStatsData.RawRxUpdateStats.Unreach)
	r["tx_update_total"] = float64(v.PeerStatsData.RawTxUpdateStats.Total)
	r["rx_update_total"] = float64(v.PeerStatsData.RawRxUpdateStats.Total)
	r["tx_update_reach"] = float64(v.PeerStatsData.RawTxUpdateStats.Reach)
	r["rx_update_reach"] = float64(v.PeerStatsData.RawRxUpdateStats.Reach)
	r["tx_update_end_of_rib"] = float64(v.PeerStatsData.RawTxUpdateStats.EndOfRib)
	r["rx_update_end_of_rib"] = float64(v.PeerStatsData.RawRxUpdateStats.EndOfRib)
	r["flap_count"] = float64(v.PeerFlapData.FlapInfo.FlapCount)
	r["flap_time"] = float64(v.PeerFlapData.FlapInfo.FlapTime)
	l = append(l, fmt.Sprintf("%v", v.BgpPeerInfoData.LocalAsn),
		fmt.Sprintf("%v", v.BgpPeerInfoData.PeerAsn),
		v.BgpPeerInfoData.RouterType,
		v.BgpPeerInfoData.PeerAddress)
	r["rx_proto_notification"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxProtoStats.Notification)
	r["rx_proto_update"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxProtoStats.Update)
	r["rx_proto_close"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxProtoStats.Close)
	r["rx_proto_total"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxProtoStats.Total)
	r["rx_proto_open"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxProtoStats.Open)
	r["rx_proto_keepalive"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxProtoStats.Keepalive)
	r["rx_update_unreach"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxUpdateStats.Unreach)
	r["rx_update_total"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxUpdateStats.Total)
	r["rx_update_reach"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxUpdateStats.Reach)
	r["rx_update_end_of_rib"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxUpdateStats.EndOfRib)
	r["tx_proto_notification"] = float64(v.BgpPeerInfoData.PeerStatsInfo.TxProtoStats.Notification)
	r["tx_proto_update"] = float64(v.BgpPeerInfoData.PeerStatsInfo.TxProtoStats.Update)
	r["tx_proto_close"] = float64(v.BgpPeerInfoData.PeerStatsInfo.TxProtoStats.Close)
	r["tx_proto_total"] = float64(v.BgpPeerInfoData.PeerStatsInfo.TxProtoStats.Total)
	r["tx_proto_open"] = float64(v.BgpPeerInfoData.PeerStatsInfo.TxProtoStats.Open)
	r["tx_proto_keepalive"] = float64(v.BgpPeerInfoData.PeerStatsInfo.TxProtoStats.Keepalive)
	r["tx_update_unreach"] = float64(v.BgpPeerInfoData.PeerStatsInfo.TxUpdateStats.Unreach)
	r["tx_update_total"] = float64(v.BgpPeerInfoData.PeerStatsInfo.TxUpdateStats.Total)
	r["tx_update_reach"] = float64(v.BgpPeerInfoData.PeerStatsInfo.TxUpdateStats.Reach)
	r["tx_update_end_of_rib"] = float64(v.BgpPeerInfoData.PeerStatsInfo.TxUpdateStats.EndOfRib)
	r["rx_route_primary_path_count"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxRouteStats.PrimaryPathCount)
	r["rx_route_total_path_count"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxRouteStats.TotalPathCount)
	r["rx_err_bad_inet6_xml_token_count"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxErrorStats.Inet6ErrorStats.BadInet6XMLTokenCount)
	r["rx_err_bad_inet6_afi_safi_count"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxErrorStats.Inet6ErrorStats.BadInet6AfiSafiCount)
	r["rx_err_bad_inet6_nexthop_count"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxErrorStats.Inet6ErrorStats.BadInet6NexthopCount)
	r["rx_err_bad_inet6_prefix_count"] = float64(v.BgpPeerInfoData.PeerStatsInfo.RxErrorStats.Inet6ErrorStats.BadInet6PrefixCount)

	return NamedMap{Name: v.Name, Map: r, Labels: l}
}

var bgp_peer_metric_desc = make(map[string]*prometheus.Desc)

func (e *TungstenBgpPeerCollector) Describe(ch chan<- *prometheus.Desc) {
	metrics := []string{
		"tx_update_unreach", "rx_update_unreach", "tx_update_total", "rx_update_total", "tx_update_reach", "rx_update_reach",
		"tx_update_end_of_rib", "rx_update_end_of_rib", "flap_count", "flap_time", "rx_proto_notification", "rx_proto_update", "rx_proto_close", "rx_proto_total",
		"rx_proto_open", "rx_proto_keepalive", "rx_update_unreach", "rx_update_total", "rx_update_reach", "rx_update_end_of_rib", "tx_proto_notification",
		"tx_proto_update", "tx_proto_close", "tx_proto_total", "tx_proto_open", "tx_proto_keepalive", "tx_update_unreach", "tx_update_total", "tx_update_reach", "tx_update_end_of_rib",
		"rx_route_primary_path_count", "rx_route_total_path_count", "rx_err_bad_inet6_xml_token_count", "rx_err_bad_inet6_afi_safi_count", "rx_err_bad_inet6_nexthop_count", "rx_err_bad_inet6_prefix_count"}
	for _, metric := range metrics {
		_, n := get_metric_type(metric)
		bgp_peer_metric_desc[metric] = prometheus.NewDesc(prometheus.BuildFQName("tf", "bgp_peer", n), "", []string{"peer_name", "local_asn", "peer_asn", "router_type", "peer_address"}, nil)
	}
}
func (e *TungstenBgpPeerCollector) Collect(ch chan<- prometheus.Metric) {
	for _, v := range bgp_peer_maps {
		for k, m := range bgp_peer_metric_desc {
			t, n := get_metric_type(k)
			ch <- prometheus.MustNewConstMetric(m, t, v.Map[n], v.Name, v.Labels[0], v.Labels[1], v.Labels[2], v.Labels[3])
		}
	}
}
