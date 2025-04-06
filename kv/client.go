package kv

import (
	uuid "github.com/satori/go.uuid"
	"gopkg.in/yaml.v3"
	"os"
	"raftkv/utils"
)

type ClientConfig struct {
	ClientEnd []struct {
		Ip   string
		Port string
	} `yaml:"servers"`
}

type Client struct {
	servers   []*utils.ClientEnd
	id        uuid.UUID
	serverLen int
	leader    int
}

func generateUUID() uuid.UUID {
	id := uuid.NewV1()
	return id
}

func MakeKVClient(servers []*utils.ClientEnd) *Client {
	client := new(Client)
	client.servers = servers
	client.id = generateUUID()
	client.serverLen = len(servers)
	return client
}

func (client *Client) Get(key string) string {
	args := &GetArgs{
		Key:    key,
		Id:     client.id,
		Serial: generateUUID(),
	}
	reply := &GetReply{}

	for {
		if ok := client.servers[client.leader].Call(RPCGet, args, reply); !ok {
			client.leader = (client.leader + 1) % client.serverLen
			continue
		}

		if reply.Err == OK {
			return reply.Value
		} else if reply.Err == ErrNoKey {
			return NoKeyValue
		} else if reply.Err == ErrWrongLeader {
			client.leader = (client.leader + 1) % client.serverLen
			continue
		} else if reply.Err == ErrLevelDB {
			panic("LevelDB Error")
		} else {
			panic("Unknown Empty Error")
		}
	}
}

func (client *Client) Put(key string, value string) {
	args := &PutArgs{
		Key:    key,
		Value:  value,
		Id:     client.id,
		Serial: generateUUID(),
	}
	reply := &PutReply{}

	for {
		if ok := client.servers[client.leader].Call(RPCPut, args, reply); !ok {
			client.leader = (client.leader + 1) % client.serverLen
			continue
		}

		if reply.Err == OK {
			return
		} else if reply.Err == ErrWrongLeader {
			client.leader = (client.leader + 1) % client.serverLen
		} else if reply.Err == ErrLevelDB {
			panic("LevelDB Error")
		} else {
			panic("Unknown Empty Error")
		}
	}
}

func (client *Client) Delete(key string) {
	args := &DeleteArgs{
		Key:    key,
		Id:     client.id,
		Serial: generateUUID(),
	}
	reply := &DeleteReply{}

	for {
		if ok := client.servers[client.leader].Call(RPCDelete, args, reply); !ok {
			client.leader = (client.leader + 1) % client.serverLen
			continue
		}

		if reply.Err == OK {
			return
		} else if reply.Err == ErrWrongLeader {
			client.leader = (client.leader + 1) % client.serverLen
			continue
		} else if reply.Err == ErrLevelDB {
			panic("LevelDB Error")
		} else {
			panic("Unknown Empty Error")
		}
	}
}

func getClientConfig(path string) *ClientConfig {
	if len(os.Args) == 2 {
		path = os.Args[1]
	}

	cfgFile, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	cfg := &ClientConfig{}
	err = yaml.Unmarshal(cfgFile, cfg)
	if err != nil {
		panic(err)
	}
	return cfg
}

func GetClientEnds(path string) []*utils.ClientEnd {
	cfg := getClientConfig(path)
	num := len(cfg.ClientEnd)
	if (num&1) == 0 || num < 3 {
		panic("the number of servers must be odd and greater than or equal to 3")
	}

	clientEnds := make([]*utils.ClientEnd, 0)
	for _, end := range cfg.ClientEnd {
		address := end.Ip + ":" + end.Port
		client := utils.TryConnect(address)

		clientEnd := &utils.ClientEnd{
			Addr:   address,
			Client: client,
		}

		clientEnds = append(clientEnds, clientEnd)
	}

	return clientEnds
}
