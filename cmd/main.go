package main

import (
	"tinydfs-master/internal/model"
	"tinydfs-master/internal/service"
)

func init() {
	model.CreateNameNode()
	service.CreateMasterHandler()
}

func main() {
	service.GlobalMasterHandler.Server()
}
