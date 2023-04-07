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
	dial, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		return err
	}
	defer dial.Close()

	log.Println("Cli dial complete")

	log.Println("Cli is trying call rpc...")

	doneChan := make(chan *rpc.Call, 10)
	resRepo := new(sync.Map)

	for _, i := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		res := new(Server.Respond)
		identifier := dial.Go("Server.RpcCallSq", i, res, doneChan)
		resRepo.Store(identifier, res)
	}
	for id := range doneChan {
		res, _ := resRepo.Load(id)
		log.Println("get:" + res.(*Server.Respond).Data)
	}

	log.Println("Cli call rpc complete")
	return nil
}
