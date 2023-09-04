package main

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type Vrouter struct {
	Name string `json:"-"`

	VrouterStatsAgent struct {
		ExceptionPackets            int     `json:"exception_packets"`
		TotalInBandwidthUtilization float64 `json:"total_in_bandwidth_utilization"`
		RawPhyIfDropStats           map[string]struct {
			DsRewriteFail            int `json:"ds_rewrite_fail"`
			DsMcastDfBit             int `json:"ds_mcast_df_bit"`
			DsFlowNoMemory           int `json:"ds_flow_no_memory"`
			DsPush                   int `json:"ds_push"`
			DsInvalidIf              int `json:"ds_invalid_if"`
			DsPull                   int `json:"ds_pull"`
			DsNoFmd                  int `json:"ds_no_fmd"`
			DsInvalidArp             int `json:"ds_invalid_arp"`
			DsTrapNoIf               int `json:"ds_trap_no_if"`
			DsVlanFwdTx              int `json:"ds_vlan_fwd_tx"`
			DsDropPkts               int `json:"ds_drop_pkts"`
			DsCksumErr               int `json:"ds_cksum_err"`
			DsInvalidSource          int `json:"ds_invalid_source"`
			DsFlowActionInvalid      int `json:"ds_flow_action_invalid"`
			DsInvalidPacket          int `json:"ds_invalid_packet"`
			DsFlowInvalidProtocol    int `json:"ds_flow_invalid_protocol"`
			DsInvalidVnid            int `json:"ds_invalid_vnid"`
			DsFlowTableFull          int `json:"ds_flow_table_full"`
			DsInvalidLabel           int `json:"ds_invalid_label"`
			DsFragErr                int `json:"ds_frag_err"`
			DsVlanFwdEnq             int `json:"ds_vlan_fwd_enq"`
			DsDropNewFlow            int `json:"ds_drop_new_flow"`
			DsDuplicated             int `json:"ds_duplicated"`
			DsNoMemory               int `json:"ds_no_memory"`
			DsMisc                   int `json:"ds_misc"`
			DsTrapOriginal           int `json:"ds_trap_original"`
			DsInterfaceRxDiscard     int `json:"ds_interface_rx_discard"`
			DsFlowUnusable           int `json:"ds_flow_unusable"`
			DsCloneFail              int `json:"ds_clone_fail"`
			DsNoFragEntry            int `json:"ds_no_frag_entry"`
			DsMcastCloneFail         int `json:"ds_mcast_clone_fail"`
			DsInvalidProtocol        int `json:"ds_invalid_protocol"`
			DsInterfaceTxDiscard     int `json:"ds_interface_tx_discard"`
			DsFlowActionDrop         int `json:"ds_flow_action_drop"`
			DsIcmpError              int `json:"ds_icmp_error"`
			DsNowhereToGo            int `json:"ds_nowhere_to_go"`
			DsL2NoRoute              int `json:"ds_l2_no_route"`
			DsFlowEvict              int `json:"ds_flow_evict"`
			DsInvalidMcastSource     int `json:"ds_invalid_mcast_source"`
			DsDiscard                int `json:"ds_discard"`
			DsFlowQueueLimitExceeded int `json:"ds_flow_queue_limit_exceeded"`
			DsFlowNatNoRflow         int `json:"ds_flow_nat_no_rflow"`
			DsInvalidNh              int `json:"ds_invalid_nh"`
			DsHeadAllocFail          int `json:"ds_head_alloc_fail"`
			DsInterfaceDrop          int `json:"ds_interface_drop"`
			DsPcowFail               int `json:"ds_pcow_fail"`
			DsTTLExceeded            int `json:"ds_ttl_exceeded"`
			DsFragmentQueueFail      int `json:"ds_fragment_queue_fail"`
		} `json:"raw_phy_if_drop_stats"`
		InBytes       int64 `json:"in_bytes"`
		RawPhyIfStats map[string]struct {
			OutBytes int64 `json:"out_bytes"`
			InBytes  int64 `json:"in_bytes"`
			InPkts   int   `json:"in_pkts"`
			OutPkts  int   `json:"out_pkts"`
		} `json:"raw_phy_if_stats"`
		RawVhostStats struct {
			OutBytes  int64  `json:"out_bytes"`
			Name      string `json:"name"`
			InBytes   int64  `json:"in_bytes"`
			Duplexity int    `json:"duplexity"`
			OutPkts   int    `json:"out_pkts"`
			InPkts    int    `json:"in_pkts"`
			Speed     int    `json:"speed"`
		} `json:"raw_vhost_stats"`
		RawVhostDropStats struct {
			DsRewriteFail            int `json:"ds_rewrite_fail"`
			DsMcastDfBit             int `json:"ds_mcast_df_bit"`
			DsFlowNoMemory           int `json:"ds_flow_no_memory"`
			DsPush                   int `json:"ds_push"`
			DsInvalidIf              int `json:"ds_invalid_if"`
			DsPull                   int `json:"ds_pull"`
			DsNoFmd                  int `json:"ds_no_fmd"`
			DsInvalidArp             int `json:"ds_invalid_arp"`
			DsTrapNoIf               int `json:"ds_trap_no_if"`
			DsVlanFwdTx              int `json:"ds_vlan_fwd_tx"`
			DsDropPkts               int `json:"ds_drop_pkts"`
			DsCksumErr               int `json:"ds_cksum_err"`
			DsInvalidSource          int `json:"ds_invalid_source"`
			DsFlowActionInvalid      int `json:"ds_flow_action_invalid"`
			DsInvalidPacket          int `json:"ds_invalid_packet"`
			DsFlowInvalidProtocol    int `json:"ds_flow_invalid_protocol"`
			DsInvalidVnid            int `json:"ds_invalid_vnid"`
			DsFlowTableFull          int `json:"ds_flow_table_full"`
			DsInvalidLabel           int `json:"ds_invalid_label"`
			DsFragErr                int `json:"ds_frag_err"`
			DsVlanFwdEnq             int `json:"ds_vlan_fwd_enq"`
			DsDropNewFlow            int `json:"ds_drop_new_flow"`
			DsDuplicated             int `json:"ds_duplicated"`
			DsNoMemory               int `json:"ds_no_memory"`
			DsMisc                   int `json:"ds_misc"`
			DsTrapOriginal           int `json:"ds_trap_original"`
			DsInterfaceRxDiscard     int `json:"ds_interface_rx_discard"`
			DsFlowUnusable           int `json:"ds_flow_unusable"`
			DsCloneFail              int `json:"ds_clone_fail"`
			DsNoFragEntry            int `json:"ds_no_frag_entry"`
			DsMcastCloneFail         int `json:"ds_mcast_clone_fail"`
			DsInvalidProtocol        int `json:"ds_invalid_protocol"`
			DsInterfaceTxDiscard     int `json:"ds_interface_tx_discard"`
			DsFlowActionDrop         int `json:"ds_flow_action_drop"`
			DsIcmpError              int `json:"ds_icmp_error"`
			DsNowhereToGo            int `json:"ds_nowhere_to_go"`
			DsL2NoRoute              int `json:"ds_l2_no_route"`
			DsFlowEvict              int `json:"ds_flow_evict"`
			DsInvalidMcastSource     int `json:"ds_invalid_mcast_source"`
			DsDiscard                int `json:"ds_discard"`
			DsFlowQueueLimitExceeded int `json:"ds_flow_queue_limit_exceeded"`
			DsFlowNatNoRflow         int `json:"ds_flow_nat_no_rflow"`
			DsInvalidNh              int `json:"ds_invalid_nh"`
			DsHeadAllocFail          int `json:"ds_head_alloc_fail"`
			DsInterfaceDrop          int `json:"ds_interface_drop"`
			DsPcowFail               int `json:"ds_pcow_fail"`
			DsTTLExceeded            int `json:"ds_ttl_exceeded"`
			DsFragmentQueueFail      int `json:"ds_fragment_queue_fail"`
		} `json:"raw_vhost_drop_stats"`
		Uptime         int64 `json:"uptime"`
		TotalFlows     int   `json:"total_flows"`
		PhyIf5MinUsage []struct {
			OutBandwidthUsage int    `json:"out_bandwidth_usage"`
			Name              string `json:"name"`
			InBandwidthUsage  int    `json:"in_bandwidth_usage"`
		} `json:"phy_if_5min_usage"`
		ExceptionPacketsDropped int `json:"exception_packets_dropped"`
		RawDropStats            struct {
			DsRewriteFail            int `json:"ds_rewrite_fail"`
			DsMcastDfBit             int `json:"ds_mcast_df_bit"`
			DsFlowNoMemory           int `json:"ds_flow_no_memory"`
			DsPush                   int `json:"ds_push"`
			DsInvalidIf              int `json:"ds_invalid_if"`
			DsPull                   int `json:"ds_pull"`
			DsNoFmd                  int `json:"ds_no_fmd"`
			DsInvalidArp             int `json:"ds_invalid_arp"`
			DsTrapNoIf               int `json:"ds_trap_no_if"`
			DsVlanFwdTx              int `json:"ds_vlan_fwd_tx"`
			DsDropPkts               int `json:"ds_drop_pkts"`
			DsCksumErr               int `json:"ds_cksum_err"`
			DsInvalidSource          int `json:"ds_invalid_source"`
			DsFlowActionInvalid      int `json:"ds_flow_action_invalid"`
			DsInvalidPacket          int `json:"ds_invalid_packet"`
			DsFlowInvalidProtocol    int `json:"ds_flow_invalid_protocol"`
			DsInvalidVnid            int `json:"ds_invalid_vnid"`
			DsFlowTableFull          int `json:"ds_flow_table_full"`
			DsInvalidLabel           int `json:"ds_invalid_label"`
			DsFragErr                int `json:"ds_frag_err"`
			DsVlanFwdEnq             int `json:"ds_vlan_fwd_enq"`
			DsDropNewFlow            int `json:"ds_drop_new_flow"`
			DsDuplicated             int `json:"ds_duplicated"`
			DsNoMemory               int `json:"ds_no_memory"`
			DsMisc                   int `json:"ds_misc"`
			DsTrapOriginal           int `json:"ds_trap_original"`
			DsInterfaceRxDiscard     int `json:"ds_interface_rx_discard"`
			DsFlowUnusable           int `json:"ds_flow_unusable"`
			DsCloneFail              int `json:"ds_clone_fail"`
			DsNoFragEntry            int `json:"ds_no_frag_entry"`
			DsMcastCloneFail         int `json:"ds_mcast_clone_fail"`
			DsInvalidProtocol        int `json:"ds_invalid_protocol"`
			DsInterfaceTxDiscard     int `json:"ds_interface_tx_discard"`
			DsFlowActionDrop         int `json:"ds_flow_action_drop"`
			DsIcmpError              int `json:"ds_icmp_error"`
			DsNowhereToGo            int `json:"ds_nowhere_to_go"`
			DsL2NoRoute              int `json:"ds_l2_no_route"`
			DsFlowEvict              int `json:"ds_flow_evict"`
			DsInvalidMcastSource     int `json:"ds_invalid_mcast_source"`
			DsDiscard                int `json:"ds_discard"`
			DsFlowQueueLimitExceeded int `json:"ds_flow_queue_limit_exceeded"`
			DsFlowNatNoRflow         int `json:"ds_flow_nat_no_rflow"`
			DsInvalidNh              int `json:"ds_invalid_nh"`
			DsHeadAllocFail          int `json:"ds_head_alloc_fail"`
			DsInterfaceDrop          int `json:"ds_interface_drop"`
			DsPcowFail               int `json:"ds_pcow_fail"`
			DsTTLExceeded            int `json:"ds_ttl_exceeded"`
			DsFragmentQueueFail      int `json:"ds_fragment_queue_fail"`
		} `json:"raw_drop_stats"`
		FlowRate struct {
			ActiveFlows             int `json:"active_flows"`
			MaxFlowDeletesPerSecond int `json:"max_flow_deletes_per_second"`
			AddedFlows              int `json:"added_flows"`
			DeletedFlows            int `json:"deleted_flows"`
			MinFlowAddsPerSecond    int `json:"min_flow_adds_per_second"`
			MinFlowDeletesPerSecond int `json:"min_flow_deletes_per_second"`
			MaxFlowAddsPerSecond    int `json:"max_flow_adds_per_second"`
			HoldFlows               int `json:"hold_flows"`
		} `json:"flow_rate"`
		OutBytes int64 `json:"out_bytes"`
	} `json:"VrouterStatsAgent"`
	VrouterAgent struct {
		VmiCount struct {
			Active int `json:"active"`
		} `json:"vmi_count"`
		VMCount struct {
			Active int `json:"active"`
		} `json:"vm_count"`
	} `json:"VrouterAgent"`
}

type VrouterMap struct {
	Map  map[string]float64
	Name string
}

func (v *Vrouter) ToMap() NamedMap {
	r := make(map[string]float64)
	r["in_bytes"] = float64(v.VrouterStatsAgent.InBytes)
	r["out_bytes"] = float64(v.VrouterStatsAgent.OutBytes)
	r["ds_rewrite_fail"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsRewriteFail)
	r["ds_mcast_df_bit"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsMcastDfBit)
	r["ds_flow_no_memory"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsFlowNoMemory)
	r["ds_push"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsPush)
	r["ds_invalid_if"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsInvalidIf)
	r["ds_pull"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsPull)
	r["ds_no_fmd"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsNoFmd)
	r["ds_invalid_arp"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsInvalidArp)
	r["ds_trap_no_if"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsTrapNoIf)
	r["ds_vlan_fwd_tx"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsVlanFwdTx)
	r["ds_drop_pkts"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsDropPkts)
	r["ds_cksum_err"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsCksumErr)
	r["ds_invalid_source"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsInvalidSource)
	r["ds_flow_action_invalid"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsFlowActionInvalid)
	r["ds_invalid_packet"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsInvalidPacket)
	r["ds_flow_invalid_protocol"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsFlowInvalidProtocol)
	r["ds_invalid_vnid"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsInvalidVnid)
	r["ds_flow_table_full"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsFlowTableFull)
	r["ds_invalid_label"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsInvalidLabel)
	r["ds_frag_err"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsFragErr)
	r["ds_vlan_fwd_enq"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsVlanFwdEnq)
	r["ds_drop_new_flow"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsDropNewFlow)
	r["ds_duplicated"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsDuplicated)
	r["ds_no_memory"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsNoMemory)
	r["ds_misc"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsMisc)
	r["ds_trap_original"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsTrapOriginal)
	r["ds_interface_rx_discard"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsInterfaceRxDiscard)
	r["ds_flow_unusable"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsFlowUnusable)
	r["ds_clone_fail"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsCloneFail)
	r["ds_no_frag_entry"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsNoFragEntry)
	r["ds_mcast_clone_fail"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsMcastCloneFail)
	r["ds_invalid_protocol"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsInvalidProtocol)
	r["ds_interface_tx_discard"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsInterfaceTxDiscard)
	r["ds_flow_action_drop"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsFlowActionDrop)
	r["ds_icmp_error"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsIcmpError)
	r["ds_nowhere_to_go"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsNowhereToGo)
	r["ds_l2_no_route"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsL2NoRoute)
	r["ds_flow_evict"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsFlowEvict)
	r["ds_invalid_mcast_source"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsInvalidMcastSource)
	r["ds_discard"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsDiscard)
	r["ds_flow_queue_limit_exceeded"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsFlowQueueLimitExceeded)
	r["ds_flow_nat_no_rflow"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsFlowNatNoRflow)
	r["ds_invalid_nh"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsInvalidNh)
	r["ds_head_alloc_fail"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsHeadAllocFail)
	r["ds_interface_drop"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsInterfaceDrop)
	r["ds_pcow_fail"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsPcowFail)
	r["ds_ttl_exceeded"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsTTLExceeded)
	r["ds_fragment_queue_fail"] = float64(v.VrouterStatsAgent.RawVhostDropStats.DsFragmentQueueFail)
	r["active_flows"] = float64(v.VrouterStatsAgent.FlowRate.ActiveFlows)
	r["max_flow_deletes_per_second"] = float64(v.VrouterStatsAgent.FlowRate.MaxFlowDeletesPerSecond)
	r["added_flows"] = float64(v.VrouterStatsAgent.FlowRate.AddedFlows)
	r["deleted_flows"] = float64(v.VrouterStatsAgent.FlowRate.DeletedFlows)
	r["min_flow_adds_per_second"] = float64(v.VrouterStatsAgent.FlowRate.MinFlowAddsPerSecond)
	r["min_flow_deletes_per_second"] = float64(v.VrouterStatsAgent.FlowRate.MinFlowDeletesPerSecond)
	r["max_flow_adds_per_second"] = float64(v.VrouterStatsAgent.FlowRate.MaxFlowAddsPerSecond)
	r["hold_flows"] = float64(v.VrouterStatsAgent.FlowRate.HoldFlows)
	r["vmi_count"] = float64(v.VrouterAgent.VmiCount.Active)
	r["vm_count"] = float64(v.VrouterAgent.VMCount.Active)
	r["total_flows"] = float64(v.VrouterStatsAgent.TotalFlows)
	r["uptime"] = float64(v.VrouterStatsAgent.Uptime)
	return NamedMap{Map: r, Name: v.Name}
}

func get_vrouters() ([]NamedMap, error) {
	hrefs, err := get_href_list("vrouter")
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
			v := Vrouter{}
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

type TungstenVrouterCollector struct {
}

var vrouter_metric_desc = make(map[string]*prometheus.Desc)

func (e *TungstenVrouterCollector) Describe(ch chan<- *prometheus.Desc) {
	metrics := []string{
		"in_bytes", "out_bytes", "ds_rewrite_fail", "ds_mcast_df_bit",
		"ds_flow_no_memory", "ds_push", "ds_invalid_if", "ds_pull",
		"ds_no_fmd", "ds_invalid_arp", "ds_trap_no_if", "ds_vlan_fwd_tx",
		"ds_vlan_fwd_tx", "ds_drop_pkts", "ds_cksum_err", "ds_invalid_source", "ds_flow_action_invalid", "ds_invalid_packet",
		"ds_flow_invalid_protocol", "ds_invalid_vnid", "ds_flow_table_full", "ds_invalid_label", "ds_frag_err", "ds_vlan_fwd_enq",
		"ds_drop_new_flow", "ds_duplicated", "ds_no_memory", "ds_misc", "ds_trap_original", "ds_interface_rx_discard",
		"ds_flow_unusable", "ds_clone_fail", "ds_no_frag_entry", "ds_no_frag_entry",
		"ds_mcast_clone_fail", "ds_invalid_protocol", "ds_interface_tx_discard", "ds_flow_action_drop",
		"ds_icmp_error", "ds_nowhere_to_go", "ds_l2_no_route", "ds_flow_evict",
		"ds_invalid_mcast_source", "ds_discard", "ds_flow_queue_limit_exceeded", "ds_flow_nat_no_rflow", "ds_invalid_nh",
		"ds_head_alloc_fail", "ds_interface_drop", "ds_pcow_fail", "ds_ttl_exceeded",
		"ds_fragment_queue_fail", "G#active_flows", "G#max_flow_deletes_per_second", "G#added_flows",
		"G#deleted_flows", "G#min_flow_adds_per_second", "G#min_flow_deletes_per_second", "G#max_flow_adds_per_second",
		"G#hold_flows", "G#vmi_count", "G#vm_count", "G#total_flows", "uptime"}
	for _, metric := range metrics {
		_, n := get_metric_type(metric)
		vrouter_metric_desc[metric] = prometheus.NewDesc(prometheus.BuildFQName("tf", "vrouter", n), "", []string{"vrouter"}, nil)
	}
}
func (e *TungstenVrouterCollector) Collect(ch chan<- prometheus.Metric) {
	for _, v := range vrouter_maps {
		for k, m := range vrouter_metric_desc {
			t, n := get_metric_type(k)
			ch <- prometheus.MustNewConstMetric(m, t, v.Map[n], v.Name)
		}
	}
}
