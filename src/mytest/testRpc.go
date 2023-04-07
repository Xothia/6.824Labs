package main

import (
	"6.5840/mytest/Client"
	"6.5840/mytest/Server"
	"fmt"
	"log"
	"time"
)

func main() {
	//os.Setenv("CGO_ENABLED", "1")
	fmt.Println("MAIN BEGIN.")
	go func() {
		RpcServer := new(Server.RpcServer)
		err := RpcServer.Run()
		if err != nil {
			log.Fatal(err)
		}
	}()
	fmt.Println("RpcServer ESTABLISHED.")

	//for i := 0; i < 10; i++ {
	//	fmt.Printf("A NEW CLI CRATED\n")
	//
	//}
	cli := new(Client.Client)
	fmt.Println("CLIENT BEGIN RUNNING.")
	go cli.RunTestExample()

	time.Sleep(time.Second * 5)
}
