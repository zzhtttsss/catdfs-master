package main

import (
	"tinydfs-master/shadow_master/internal"
)

func init() {
	internal.CreateSMHandler()
}

func main() {
	internal.GlobalSMHandler.Server()
}
