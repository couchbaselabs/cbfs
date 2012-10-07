package main

import (
	"flag"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/miekg/dns"
)

var dnsBinding = flag.String("dnsbind", "",
	"address to bind dns server to")
var dnsZone = flag.String("dnszone", "cbfs.",
	"DNS Zone for msgs/responses")

const cbfsSvc = "_cbfs._tcp"

const maxDnsResponses = 8

type dnsService struct{}

func (d dnsService) serviceDomain() string {
	return cbfsSvc + "." + *dnsZone
}

func (d dnsService) writeLogErr(w dns.ResponseWriter, msg *dns.Msg) {
	err := w.Write(msg)
	if err != nil {
		log.Printf("Error writing response: %v\n%v", err, msg)
	}
}

func (d dnsService) srvList(w dns.ResponseWriter, r *dns.Msg) {
	msg := &dns.Msg{}

	nl, err := findAllNodes()
	if err != nil {
		log.Printf("Error finding nodes: %v", err)
		return
	}

	for i, n := range nl {
		_, p, err := net.SplitHostPort(n.Address())
		if err != nil {
			continue
		}
		port := 8484
		tmp, err := strconv.Atoi(p)
		if err == nil {
			port = tmp
		}

		rr := &dns.RR_SRV{
			Hdr: dns.RR_Header{
				Name:   d.serviceDomain(),
				Rrtype: dns.TypeSRV,
				Class:  dns.ClassINET,
				Ttl:    5},
			Priority: uint16(i),
			Weight:   uint16(time.Since(n.Time).Seconds()),
			Port:     uint16(port),
			Target:   n.name + "." + *dnsZone,
		}
		msg.Answer = append(msg.Answer, rr)

		arr := &dns.RR_A{
			Hdr: dns.RR_Header{
				Name:   n.name + "." + *dnsZone,
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET,
				Ttl:    60,
			},
			A: net.ParseIP(n.Addr),
		}

		msg.Extra = append(msg.Extra, arr)

		if len(msg.Answer) > maxDnsResponses {
			break
		}
	}

	msg.SetReply(r)
	d.writeLogErr(w, msg)
}

func (d dnsService) hostLookup(w dns.ResponseWriter, r *dns.Msg) {
	msg := &dns.Msg{}

	name := r.Question[0].Name
	name = name[:len(name)-len(*dnsZone)-1]

	node, err := findNode(name)
	if err == nil {
		msg.Answer = []dns.RR{&dns.RR_A{
			Hdr: dns.RR_Header{
				Name:   r.Question[0].Name,
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET,
				Ttl:    60,
			},
			A: net.ParseIP(node.Addr),
		}}
		msg.SetReply(r)
	} else {
		msg.SetRcode(r, dns.RcodeNameError)
	}

	d.writeLogErr(w, msg)
}

func (d dnsService) listHosts(w dns.ResponseWriter, r *dns.Msg) {
	msg := &dns.Msg{}

	nl, err := findAllNodes()
	if err != nil {
		log.Printf("Error finding nodes: %v", err)
		return
	}

	for _, n := range nl {
		if time.Since(n.Time) > (3 * globalConfig.HeartbeatFreq) {
			continue
		}
		rr := &dns.RR_A{
			Hdr: dns.RR_Header{
				Name:   *dnsZone,
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET,
				Ttl:    5},
			A: net.ParseIP(n.Addr),
		}

		msg.Answer = append(msg.Answer, rr)
		if len(msg.Answer) > maxDnsResponses {
			break
		}
	}

	msg.SetReply(r)
	d.writeLogErr(w, msg)
}

func (d dnsService) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	if *verbose {
		log.Printf("Incoming DNS Query: %v", r)
	}
	q := dns.Question{}

	if len(r.Question) == 1 &&
		strings.HasSuffix(r.Question[0].Name, *dnsZone) {
		q = r.Question[0]
	}

	switch q.Qtype {
	case dns.TypeSRV:
		d.srvList(w, r)
	case dns.TypeA, dns.TypeANY:
		if q.Name == *dnsZone {
			d.listHosts(w, r)
		} else {
			d.hostLookup(w, r)
		}
	case dns.TypeAAAA:
		msg := &dns.Msg{}
		msg.SetRcode(r, dns.RcodeNameError)
		d.writeLogErr(w, msg)
	default:
		msg := &dns.Msg{}
		msg.SetRcode(r, dns.RcodeNotImplemented)
		d.writeLogErr(w, msg)
	}
}

func dnsServices() {
	if *dnsBinding == "" {
		return
	}

	log.Printf("Starting DNS services on %v.", *dnsBinding)

	d := dnsService{}

	serv := dns.Server{
		Net:     "udp",
		Addr:    *dnsBinding,
		Handler: d,
	}

	err := serv.ListenAndServe()
	if err != nil {
		log.Fatalf("DNS server failure: %v", err)
	}
}
