package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"go.etcd.io/raft/v3/example/server"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Uint64("id", 1, "node ID")
	kvport := flag.Int("port", 9021, "key-value server port")
	logLevel := flag.String("log-level", "info", "You can set the logging level")
	logCaller := flag.Bool("log-caller", false, "You can set the logging Caller")
	//join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	newLogLevel, err := log.ParseLevel(*logLevel)
	if err != nil {
		newLogLevel = log.InfoLevel
	}

	log.SetLevel(newLogLevel)
	if *logCaller {
		log.SetReportCaller(true)
	}

	var members = []server.Member{}
	for i, v := range strings.Split(*cluster, ",") {
		members = append(members, server.Member{
			NodeID:  uint64(i + 1),
			Host:    v,
			Learner: false,
		})
	}
	cfg := &server.Config{
		NodeId:       *id,
		ListenPort:   *kvport,
		TickInterval: 500 * time.Millisecond,
		ElectionTick: 5,
		Members:      members,
	}
	fmt.Println(cfg.Members)

	if err := cfg.Verify(); err != nil {
		log.Fatalf("config verify err: %v", err)
	}

	log.Infof("cfg: %s", cfg)

	svr := server.NewServer(cfg)
	svr.Start()

	sig := waitForSigterm()
	log.Infof("service received signal %s", sig)
	log.Infof("gracefully shutting down http service at :%v", *kvport)

	svr.Stop()
	log.Infof("successfully shut down the service")
}

func waitForSigterm() os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	for {
		sig := <-ch
		return sig
	}
}
