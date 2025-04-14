package kv

const (
	DELAY = iota
	DROP
	NORM
)

//
//func main() {
//	const address = "127.0.0.1:10002"
//	client := utils.TryConnect(address)
//	client_end := &utils.ClientEnd{
//		Addr:   address,
//		Client: client,
//	}
//
//	r := rand.New(rand.NewSource(time.Now().UnixNano()))
//	randBug := r.Float64()
//	if randBug < 0.1 {
//		rpcState := DELAY
//	} else if randBug < 0.2 {
//		rpcState := DROP
//	} else {
//		rpcState := NORM
//	}
//
//}
