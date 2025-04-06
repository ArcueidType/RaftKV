package main

import (
	"gopkg.in/yaml.v3"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"raftkv/kv"
	"raftkv/raft"
	"raftkv/utils"
)

type Config struct {
	Servers []struct {
		Ip   string
		Port string
	}
	Me int `yaml:"me"`
}

func getConfig() Config {
	var path string
	if len(os.Args) == 1 {
		panic("Missing config file path")
	} else {
		path = os.Args[1]
	}

	cfgFile, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	config := Config{}
	err = yaml.Unmarshal(cfgFile, &config)
	if err != nil {
		panic(err)
	}

	return config
}

func getClientEnds(cfg Config) []*utils.ClientEnd {
	clientEnds := make([]*utils.ClientEnd, 0)

	for i, end := range cfg.Servers {
		address := end.Ip + ":" + end.Port
		var client *rpc.Client
		if i == cfg.Me {
			client = nil
		} else {
			client = utils.TryConnect(address)
		}

		clientEnd := &utils.ClientEnd{
			Addr:   address,
			Client: client,
		}

		clientEnds = append(clientEnds, clientEnd)
	}

	return clientEnds
}

func main() {
	serverCfg := getConfig()
	serverNum := len(serverCfg.Servers)
	if (serverNum & 1) == 0 {
		panic("the number of servers must be odd")
	}
	clientEnds := getClientEnds(serverCfg)
	persister := raft.MakePersister(serverCfg.Me)
	server := kv.StartKVServer(clientEnds, serverCfg.Me, persister)

	if err := rpc.Register(server); err != nil {
		panic(err)
	}

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+serverCfg.Servers[serverCfg.Me].Port)
	if err != nil {
		panic(err)
	}
	log.Println("Listen: ", serverCfg.Servers[serverCfg.Me].Port)
	http.Serve(listener, nil)
}
