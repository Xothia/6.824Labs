package Server

//对外提供服务
import (
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
)

type Server int
type RpcServer int

type Respond struct {
	Id   string
	Data string
}

func (ser *Server) RpcCallSq(req int, res *Respond) error {
	res.Id = strconv.Itoa(200)
	res.Data = strconv.Itoa(req * req)
	return nil
}

func (rpcSer *RpcServer) Run() error {
	ser := new(Server)
	err := rpc.Register(ser)
	if err != nil {
		return err
	}
	os.Remove("../socketFile")
	listener, err := net.Listen("unix", "../socketFile")
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Println("rpc Server begin listen")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error: ", err)
		}
		go rpc.ServeConn(conn)
	}
	return nil
}
