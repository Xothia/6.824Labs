package Client

import (
	"6.5840/mytest/Server"
	"log"
	"net/rpc"
	"sync"
)

// 调用远程服务
type Client int

func (cli *Client) RunTestExample() error {
	log.Println("Cli is trying dial...")
	dial, err := rpc.DialHTTP("tcp", "localhost:1234")
	if err != nil {
		return err
	}

	defer dial.Close()
	log.Println("Cli dial complete")

	log.Println("Cli is trying call rpc...")

	doneChan := make(chan *rpc.Call, 10)
	//resRepo := make(map[*rpc.Call]*Server.Respond)
	resRepo := new(sync.Map)

	for _, i := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		//err = dial.Call("Server.RpcCallSq", i, &respond)
		res := new(Server.Respond)
		identifier := dial.Go("Server.RpcCallSq", i, res, doneChan)
		resRepo.Store(identifier, res)
		//resRepo[identifier] = res
		//log.Println("get respond:" + respond.Data)
	}
	for id := range doneChan {
		//log.Println("get:" + resRepo[id].Data)
		res, _ := resRepo.Load(id)
		log.Println("get:" + res.(Server.Respond).Data)
	}

	log.Println("Cli call rpc complete")
	return nil
}
