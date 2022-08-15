package model

import (
	"context"
	"log"
	"time"
	"tinydfs-master/internal/service"

	"google.golang.org/grpc/peer"
)

type DataNode struct {
	Id        int
	alive     int // 0 died ; 1 alive ; 2 waiting
	waittimer *time.Timer
	dietimer  *time.Timer
}

// Register 由DataNode调用该方法，将对应DataNode注册到本NameNode上
func (nn *NameNode) Register(ctx context.Context, args *service.DNRegisterArgs) (*service.DNRegisterReply, error) {
	rep := new(service.DNRegisterReply)
	p, _ := peer.FromContext(ctx)
	select {
	case t := <-nn.idChan:
		rep.Id = uint32(t)
	default:
		nn.getId()
		rep.Id = uint32(<-nn.idChan)
	}
	rep.Addr = p.Addr.String()
	nn.mu.Lock()
	nn.NumDataNode++
	nn.mu.Unlock()

	//定时器，10秒无心跳则等待重连，十分钟无心跳则判定离线
	waittimer := time.NewTimer(10 * time.Second)
	dietimer := time.NewTimer(1 * time.Minute)
	nn.DataNodeList[rep.Id] = &DataNode{
		Id:        int(rep.Id),
		alive:     1,
		waittimer: waittimer,
		dietimer:  dietimer,
	}

	dietimer.Stop()
	go func() {
		for {
			<-waittimer.C
			nn.DataNodeList[rep.Id].alive = 2
			log.Println("ID: ", rep.Id, " is waiting reconnect")
			waittimer.Stop()
			dietimer.Reset(1 * time.Minute)
		}
	}()
	go func() {
		<-dietimer.C
		nn.DataNodeList[rep.Id] = nil
		dietimer.Stop()
		nn.idChan <- int(rep.Id)
		log.Println("ID: ", rep.Id, " is died")
	}()

	log.Println("ID: ", rep.Id, " is connected")
	return rep, nil
}

//向idchan放入id
func (nn *NameNode) getId() {
	for i := 0; i < 3; i++ {
		nn.idChan <- nn.idIncrease
		nn.idIncrease++
	}
	if nn.idIncrease > len(nn.DataNodeList)-1 {
		nn.DataNodeList = append(nn.DataNodeList, make([]*DataNode, len(nn.DataNodeList))...)
	}
}