package main

import "tinydfs-master/internal/model"

func main() {
	master := model.MakeNameNode()
	master.Server()
}
