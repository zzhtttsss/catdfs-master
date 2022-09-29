package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
	"tinydfs-base/config"
	"tinydfs-base/util"

	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

var GlobalMasterHandler *MasterHandler

type MasterHandler struct {
	ClientCon *grpc.ClientConn
	pb.UnimplementedRegisterServiceServer
	pb.UnimplementedHeartbeatServiceServer
	pb.UnimplementedMasterAddServiceServer
	pb.UnimplementedMasterMkdirServiceServer
	pb.UnimplementedMasterMoveServiceServer
	pb.UnimplementedMasterRemoveServiceServer
	pb.UnimplementedRaftServiceServer
	FSM                   *MasterFSM
	Raft                  *raft.Raft
	FollowerStateObserver *raft.Observer
	MonitorChan           chan bool
	EtcdClient            *clientv3.Client
	raftAddress           string
}

// CreateMasterHandler Create a global MasterHandler which will handle all incoming requests.
func CreateMasterHandler() {
	var err error
	config.InitConfig()
	GlobalMasterHandler = &MasterHandler{
		FSM: &MasterFSM{},
	}
	GlobalMasterHandler.EtcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{viper.GetString(common.EtcdEndPoint)},
		DialTimeout: 5 * time.Second,
	})
	err = GlobalMasterHandler.initRaft()
	if err != nil {
		logrus.Panicf("fail to init raft, error detail : %s", err.Error())
	}
}

// initRaft Initial the raft config of the MasterHandler.
func (handler *MasterHandler) initRaft() error {
	raftConfig := raft.DefaultConfig()
	raftConfig.SnapshotInterval = 20 * time.Second
	raftConfig.SnapshotThreshold = 128
	raftConfig.Logger = hclog.L()

	localIP, err := util.GetLocalIP()
	if err != nil {
		return err
	}
	handler.raftAddress = localIP + common.AddressDelimiter + viper.GetString(common.MasterRaftPort)
	raftConfig.LocalID = raft.ServerID(handler.raftAddress)

	handler.MonitorChan = make(chan bool, 1)
	raftConfig.NotifyCh = handler.MonitorChan
	go handler.monitorCluster()

	addr, err := net.ResolveTCPAddr(common.TCP, handler.raftAddress)
	if err != nil {
		logrus.Errorf("Fail to resolve TCP address, error detail: %s", err.Error())
		return err
	}
	tcpTransport, err := raft.NewTCPTransport(handler.raftAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		logrus.Errorf("Fail to create TCP Transport, error detail: %s", err.Error())

		return err
	}

	raftDir := viper.GetString(common.MasterRaftDir)
	fss, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		logrus.Errorf("Fail to create FileSnapshotStore, error detail: %s", err.Error())
		return err
	}

	logDB, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "logs.dat"))
	if err != nil {
		logrus.Errorf("Fail to create log DB, error detail: %s", err.Error())
		return err
	}
	stableDB, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable.dat"))
	if err != nil {
		logrus.Errorf("Fail to create stable DB, error detail: %s", err.Error())
		return err
	}

	r, err := raft.NewRaft(raftConfig, handler.FSM, logDB, stableDB, fss, tcpTransport)
	if err != nil {
		logrus.Errorf("Fail to create raft node, error detail: %s", err.Error())
		return err
	}
	handler.Raft = r
	err = handler.BootstrapOrJoinCluster()
	if err != nil {
		logrus.Errorf("Fail to bootstrap or join, error detail: %s", err.Error())
		return err
	}
	return nil
}

// BootstrapOrJoinCluster Bootstrap cluster when there is no cluster, otherwise join cluster.
func (handler *MasterHandler) BootstrapOrJoinCluster() error {
	ctx := context.Background()
	kv := clientv3.NewKV(GlobalMasterHandler.EtcdClient)
	getResp, err := kv.Get(ctx, common.LeaderAddressKey)
	if err != nil {
		logrus.Errorf("Fail to get kv when init, error detail: %s", err.Error())
		return err
	}
	if len(getResp.Kvs) == 0 {
		logrus.Infof("No cluster, start to bootstrap cluster")
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(handler.raftAddress),
					Address: raft.ServerAddress(handler.raftAddress),
				},
			},
		}
		f := handler.Raft.BootstrapCluster(configuration)
		if err := f.Error(); err != nil {
			logrus.Errorf("Fail to bootstrap, error detail: %s", err.Error())
		}
		_, err = kv.Put(ctx, common.LeaderAddressKey, handler.raftAddress)
		if err != nil {
			logrus.Errorf("Fail to get kv when init, error detail: %s", err.Error())
			//todo 失败重试
		}
	} else {
		logrus.Infof("Already have cluster, start to join cluster")
		_, err := handler.joinCluster(&pb.JoinClusterArgs{})
		if err != nil {
			logrus.Errorf("Fail to join cluster, error detail: %s", err.Error())
			return err
		}
	}
	return nil
}

// joinCluster Called by follower.
// Join itself to the cluster.
func (handler *MasterHandler) joinCluster(args *pb.JoinClusterArgs) (*pb.JoinClusterReply, error) {
	ctx := context.Background()
	kv := clientv3.NewKV(GlobalMasterHandler.EtcdClient)
	getResp, err := kv.Get(ctx, common.LeaderAddressKey)
	if err != nil {
		logrus.Errorf("Fail to get key-value when join cluster, error deatil: %s", err.Error())
	}
	addr := string(getResp.Kvs[0].Value)
	addr = strings.Split(addr, common.AddressDelimiter)[0] + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewRaftServiceClient(conn)
	reply, err := client.JoinCluster(ctx, args)
	if err != nil {
		logrus.Errorf("Fail to join cluster, error detail : %s", err.Error())
	}
	return reply, err
}

// JoinCluster Called by follower.
// leader will join a follower to the cluster.
func (handler *MasterHandler) JoinCluster(ctx context.Context, args *pb.JoinClusterArgs) (*pb.JoinClusterReply, error) {
	p, _ := peer.FromContext(ctx)
	address := strings.Split(p.Addr.String(), common.AddressDelimiter)[0]
	rAddress := address + common.AddressDelimiter + viper.GetString(common.MasterRaftPort)
	logrus.Infof("Get request to join cluster, address : %s", rAddress)
	cf := handler.Raft.GetConfiguration()

	if err := cf.Error(); err != nil {
		logrus.Errorf("Fail to get raft config, error detail : %s", err.Error())
		return nil, err
	}

	for _, server := range cf.Configuration().Servers {
		if server.ID == raft.ServerID(rAddress) {
			logrus.Errorf("Node already joined raft cluster, address : %s", address)
			return nil, fmt.Errorf("node already joined raft cluster, address : %s", address)
		}
	}

	f := handler.Raft.AddVoter(raft.ServerID(rAddress), raft.ServerAddress(rAddress), 0, 0)
	if err := f.Error(); err != nil {
		logrus.Errorf("fail to add voter, address : %s, error deatail : %s", address, err.Error())
		return nil, err
	}

	logrus.Infof("node joined successfully, address : %s", address)
	return &pb.JoinClusterReply{}, nil
}

// monitorCluster Run in a goroutine.
// This function will monitor the change of current master's state (leader -> follower, follower -> leader).
// When current master become the leader of the cluster, it will change leader address in etcd and add an observer to
// observe follower state.
func (handler *MasterHandler) monitorCluster() {
	for {
		select {
		case isLeader := <-handler.MonitorChan:
			if isLeader {
				kv := clientv3.NewKV(handler.EtcdClient)
				_, err := kv.Put(context.Background(), common.LeaderAddressKey, handler.raftAddress)
				if err != nil {
					logrus.Errorf("Fail to put kv when leader change")
				}
				handler.FollowerStateObserver = getFollowerStateObserver()
				handler.Raft.RegisterObserver(handler.FollowerStateObserver)
				logrus.Infof("Become leader, success to change etcd leader infomation")
			} else {
				handler.Raft.DeregisterObserver(handler.FollowerStateObserver)
				logrus.Infof("Become follower")
			}
		}
	}
}

// getFollowerStateObserver return an Observer which can observe the follower state.
func getFollowerStateObserver() *raft.Observer {
	observerChan := make(chan raft.Observation)
	// filterFn will filter FailedHeartbeatObservation from all incoming Observation and handle them.
	filterFn := func(o *raft.Observation) bool {
		ob, ok := o.Data.(raft.FailedHeartbeatObservation)
		if ok {
			GlobalMasterHandler.Raft.RemoveServer(ob.PeerID, 0, 0)
			logrus.Infof("Remove a dead follower, address: %s", string(ob.PeerID))
		}
		return ok
	}
	return raft.NewObserver(observerChan, false, filterFn)
}

// Heartbeat 由Chunkserver调用该方法，维持心跳
func (handler *MasterHandler) Heartbeat(ctx context.Context, args *pb.HeartbeatArgs) (*pb.HeartbeatReply, error) {
	logrus.WithContext(ctx).Infof("[Id=%s] Get heartbeat.", args.Id)
	err := DoHeartbeat(args.Id)
	if err != nil {
		logrus.Errorf("Fail to heartbeat, error code: %v, error detail: %s,", common.MasterHeartbeatFailed, err.Error())
		details, _ := status.New(codes.NotFound, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterHeartbeatFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	rep := &pb.HeartbeatReply{}
	return rep, nil
}

// Register 由Chunkserver调用该方法，将对应DataNode注册到本NameNode上
func (handler *MasterHandler) Register(ctx context.Context, args *pb.DNRegisterArgs) (*pb.DNRegisterReply, error) {
	id, err := DoRegister(ctx)
	if err != nil {
		logrus.Errorf("Fail to register, error code: %v, error detail: %s,", common.MasterRegisterFailed, err.Error())
		details, _ := status.New(codes.NotFound, "").WithDetails(&pb.RPCError{
			Code: common.MasterRegisterFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	rep := &pb.DNRegisterReply{
		Id: id,
	}
	return rep, nil
}

// CheckArgs4Add Called by client.
// Check whether the path and file name entered by the user in the Add operation are legal.
func (handler *MasterHandler) CheckArgs4Add(ctx context.Context, args *pb.CheckArgs4AddArgs) (*pb.CheckArgs4AddReply, error) {
	logrus.WithContext(ctx).Infof("Get request for check add args from client, path: %s, filename: %s, size: %d", args.Path, args.FileName, args.Size)
	client := pb.NewSendOperationServiceClient(handler.ClientCon)
	op := OperationAdd(args.Path, true, args.FileName, args.Size)
	_, err := client.SendOperation(context.Background(), op)
	if err != nil {
		logrus.Errorf("Fail to store opertion in the file, error code: %v, error detail: %s,", common.MasterCheckArgs4AddFailed, err.Error())
		details, _ := status.New(codes.Unknown, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckArgs4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	fileNodeId, chunkNum, err := DoCheckArgs4Add(args)
	logrus.Infof("fileNodeId[%s] with chunkNum[%d]", fileNodeId, chunkNum)
	if err != nil {
		logrus.Errorf("Fail to check path and filename for add operation, error code: %v, error detail: %s,", common.MasterCheckArgs4AddFailed, err.Error())
		details, _ := status.New(codes.InvalidArgument, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckArgs4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	rep := &pb.CheckArgs4AddReply{
		FileNodeId: fileNodeId,
		ChunkNum:   chunkNum,
		Uuid:       op.Uuid,
	}
	return rep, nil

}

// GetDataNodes4Add Called by client.
// Allocate some DataNode to store a Chunk and select the primary DataNode
func (handler *MasterHandler) GetDataNodes4Add(ctx context.Context, args *pb.GetDataNodes4AddArgs) (*pb.GetDataNodes4AddReply, error) {
	logrus.WithContext(ctx).Infof("Get request for getting dataNodes for single chunk from client, FileNodeId: %s, ChunkIndex: %d", args.FileNodeId, args.ChunkIndex)
	dataNodes, primaryNode, err := DoGetDataNodes4Add(args.FileNodeId, args.ChunkIndex)
	if err != nil {
		logrus.Errorf("Fail to get dataNodes for single chunk for add operation, error code: %v, error detail: %s,", common.MasterGetDataNodes4AddFailed, err.Error())
		details, _ := status.New(codes.InvalidArgument, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterGetDataNodes4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	rep := &pb.GetDataNodes4AddReply{
		DataNodes:   dataNodes,
		PrimaryNode: primaryNode,
	}
	logrus.WithContext(ctx).Infof("Success to get dataNodes for single chunk for add operation, FileNodeId: %s, ChunkIndex: %d", args.FileNodeId, args.ChunkIndex)
	return rep, nil
}

// UnlockDic4Add Called by client.
// Unlock all FileNode in the target path which is used to add file.
func (handler *MasterHandler) UnlockDic4Add(ctx context.Context, args *pb.UnlockDic4AddArgs) (*pb.UnlockDic4AddReply, error) {
	logrus.WithContext(ctx).Infof("Get request for unlocking FileNodes in the target path from client, FileNodeId: %s", args.FileNodeId)
	err := DoUnlockDic4Add(args.FileNodeId, false)
	if err != nil {
		logrus.Errorf("Fail to unlock FileNodes in the target path, error code: %v, error detail: %s,", common.MasterUnlockDic4AddFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterUnlockDic4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	client := pb.NewSendOperationServiceClient(handler.ClientCon)
	_, err = client.FinishOperation(context.Background(), &pb.OperationArgs{
		Uuid:     args.OperationUuid,
		IsFinish: true,
	})
	if err != nil {
		logrus.Errorf("Finish Opeartion Failed, error code: %v, error detail: %s,", common.MasterFinishOperationFailed, err.Error())
		details, _ := status.New(codes.Unknown, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterGetDataNodes4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	rep := &pb.UnlockDic4AddReply{}
	logrus.WithContext(ctx).Infof("Success to unlock FileNodes in the target path, FileNodeId: %s", args.FileNodeId)
	return rep, nil
}

// ReleaseLease4Add Called by client.
// Release the lease of a chunk.
func (handler *MasterHandler) ReleaseLease4Add(ctx context.Context, args *pb.ReleaseLease4AddArgs) (*pb.ReleaseLease4AddReply, error) {
	logrus.WithContext(ctx).Infof("Get request for releasing the lease of a chunk from client, chunkId: %s", args.ChunkId)
	err := DoReleaseLease4Add(args.ChunkId)
	if err != nil {
		logrus.Errorf("Fail to release the lease of a chunk, error code: %v, error detail: %s,", common.MasterReleaseLease4AddFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterReleaseLease4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	rep := &pb.ReleaseLease4AddReply{}
	logrus.WithContext(ctx).Infof("Success to release the lease of a chunk, chunkId: %s", args.ChunkId)
	return rep, nil

}

// CheckAndMkdir Called by client.
// Check args and make directory at target path.
func (handler *MasterHandler) CheckAndMkdir(ctx context.Context, args *pb.CheckAndMkDirArgs) (*pb.CheckAndMkDirReply, error) {
	logrus.WithContext(ctx).Infof("Get request for checking args and make directory at target path from client, path: %s, dirName: %s", args.Path, args.DirName)
	operation := &MkdirOperation{
		Id:       util.GenerateUUIDString(),
		Des:      args.Path,
		FileName: args.DirName,
		Type:     Operation_Mkdir,
	}
	operationBytes, err := json.Marshal(operation)
	if err != nil {
		logrus.Errorf("Fail to check args and make directory at target path, error code: %v, error detail: %s,", common.MasterCheckAndMkdirFailed, err.Error())
		return nil, err
	}

	applyFuture := handler.Raft.Apply(operationBytes, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		logrus.Errorf("Fail to check args and make directory at target path, error code: %v, error detail: %s,", common.MasterCheckAndMkdirFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndMkdirFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	rep := &pb.CheckAndMkDirReply{}
	logrus.WithContext(ctx).Infof("Success to check args and make directory at target path from client, path: %s, dirName: %s", args.Path, args.DirName)
	return rep, nil
}

// CheckAndMove Called by client.
// Check args and move directory or file to target path.
func (handler *MasterHandler) CheckAndMove(ctx context.Context, args *pb.CheckAndMoveArgs) (*pb.CheckAndMoveReply, error) {
	logrus.WithContext(ctx).Infof("Get request for checking args and move directory or file to target path from client, sourcePath: %s, targetPath: %s", args.SourcePath, args.TargetPath)
	client := pb.NewSendOperationServiceClient(handler.ClientCon)
	op := OperationMove(args.SourcePath, args.TargetPath)
	_, err := client.SendOperation(context.Background(), op)
	if err != nil {
		logrus.Errorf("Fail to store opertion in the file, error code: %v, error detail: %s,", common.MasterCheckAndMoveFailed, err.Error())
		details, _ := status.New(codes.Unknown, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndMoveFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	err = DoCheckAndMove(args.SourcePath, args.TargetPath)
	if err != nil {
		logrus.Errorf("Fail to check args and move directory or file to target path, error code: %v, error detail: %s,", common.MasterCheckAndMoveFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndMoveFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	client = pb.NewSendOperationServiceClient(handler.ClientCon)
	_, err = client.FinishOperation(context.Background(), &pb.OperationArgs{
		Uuid:     op.Uuid,
		IsFinish: true,
	})
	if err != nil {
		logrus.Errorf("Finish Opeartion Failed, error code: %v, error detail: %s,", common.MasterCheckAndMoveFailed, err.Error())
		details, _ := status.New(codes.Unknown, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndMoveFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	rep := &pb.CheckAndMoveReply{}
	logrus.WithContext(ctx).Infof("Success to check args and move directory or file to target path from client, sourcePath: %s, targetPath: %s", args.SourcePath, args.TargetPath)
	return rep, nil
}

// CheckAndRemove Called by client.
// Check args and remove directory or file at target path.
func (handler *MasterHandler) CheckAndRemove(ctx context.Context, args *pb.CheckAndRemoveArgs) (*pb.CheckAndRemoveReply, error) {
	logrus.WithContext(ctx).Infof("Get request for checking args and remove directory or file at target path from client, path: %s", args.Path)
	client := pb.NewSendOperationServiceClient(handler.ClientCon)
	op := OperationRemove(args.Path)
	_, err := client.SendOperation(context.Background(), op)
	if err != nil {
		logrus.Errorf("Fail to store opertion in the file, error code: %v, error detail: %s,", common.MasterCheckAndRemoveFailed, err.Error())
		details, _ := status.New(codes.Unknown, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndRemoveFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	err = DoCheckAndRemove(args.Path)
	if err != nil {
		logrus.Errorf("Fail to check args and remove directory or file at target path, error code: %v, error detail: %s,", common.MasterCheckAndRemoveFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndRemoveFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	client = pb.NewSendOperationServiceClient(handler.ClientCon)
	_, err = client.FinishOperation(context.Background(), &pb.OperationArgs{
		Uuid:     op.Uuid,
		IsFinish: true,
	})
	if err != nil {
		logrus.Errorf("Finish Opeartion Failed, error code: %v, error detail: %s,", common.MasterCheckAndRemoveFailed, err.Error())
		details, _ := status.New(codes.Unknown, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndRemoveFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	rep := &pb.CheckAndRemoveReply{}
	logrus.WithContext(ctx).Infof("Success to check args and remove directory or file at target path from client, path: %s", args.Path)
	return rep, nil
}

func (handler *MasterHandler) Server() {
	listener, err := net.Listen(common.TCP, viper.GetString(common.MasterPort))
	if err != nil {
		logrus.Errorf("Fail to server, error code: %v, error detail: %s,", common.MasterRPCServerFailed, err.Error())
		os.Exit(1)
	}
	server := grpc.NewServer()
	pb.RegisterRegisterServiceServer(server, handler)
	pb.RegisterHeartbeatServiceServer(server, handler)
	pb.RegisterMasterAddServiceServer(server, handler)
	pb.RegisterMasterMkdirServiceServer(server, handler)
	pb.RegisterMasterMoveServiceServer(server, handler)
	pb.RegisterMasterRemoveServiceServer(server, handler)
	pb.RegisterRaftServiceServer(server, handler)
	logrus.Infof("Master is running, listen on %s%s", common.LocalIP, viper.GetString(common.MasterPort))
	server.Serve(listener)
}
