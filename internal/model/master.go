package model

import (
	"github.com/spf13/viper"
	"log"
	"net"
	"sync"
	"tinydfs-master/config"
	"tinydfs-master/internal/service"

	"google.golang.org/grpc"
)

type NameNode struct {
	NumDataNode  int
	idChan       chan int
	idIncrease   int
	DataNodeList []*DataNode
	mu           *sync.Mutex

	service.UnimplementedRegisterServiceServer
}

//MakeNameNode 创建NameNode
func MakeNameNode() *NameNode {
	config.Config()

	nn := NameNode{
		idChan:       make(chan int, 10),
		DataNodeList: make([]*DataNode, 16, 128),
		mu:           &sync.Mutex{},
	}
	return &nn
}

//rpc挂载
func (nn *NameNode) Server() {
	l, e := net.Listen("tcp", viper.GetString("master.port"))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	s := grpc.NewServer()
	service.RegisterRegisterServiceServer(s, nn)
	log.Println("NameNode is running, listen on " + "127.0.0.1" + viper.GetString("master.port"))
	s.Serve(l)
}
