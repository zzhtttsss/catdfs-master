package main

import (
	"fmt"
	"github.com/spf13/viper"
	"tinydfs-base/config"
)

func main() {
	config.InitConfig()
	fmt.Println(viper.GetString("shadowMaster.addr"))
}
