package main

import (
	"tinydfs-master/internal"
)

func init() {
	internal.CreateMasterHandler()
}

func main() {
	internal.GlobalMasterHandler.Server()
}
