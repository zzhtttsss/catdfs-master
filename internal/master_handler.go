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
	"tinydfs-base/util"

	"tinydfs-base/common"
	"tinydfs-base/config"
	"tinydfs-base/protocol/pb"
)

var GlobalMasterHandler *MasterHandler

const (
	raftPort = "2345"
	//raftPort      = "2346"
	maxErrorCount = 3
	raftDir       = "./raftData"
	//raftDir = "./raftData1"
	etcdEndPoint = "172.18.0.20:2379"
	etcdKey      = "leader-address"
	tcp          = "tcp"
	masterPort   = "9099"
)

type MasterHandler struct {
	ClientCon *grpc.ClientConn
	pb.UnimplementedRegisterServiceServer
	pb.UnimplementedHeartbeatServiceServer
	pb.UnimplementedMasterAddServiceServer
	pb.UnimplementedMasterMkdirServiceServer
	pb.UnimplementedMasterMoveServiceServer
	pb.UnimplementedMasterRemoveServiceServer
	pb.UnimplementedRaftServiceServer
	FSM         *MasterFSM
	Raft        *raft.Raft
	MonitorChan chan bool
	EtcdClient  *clientv3.Client
	raftAddress string
}

//CreateMasterHandler 创建MasterHandler
func CreateMasterHandler() {
	//raft.NewRaft()
	config.InitConfig()
	rootMap := ReadRootLines(common.DirectoryFileName)
	if rootMap != nil {
		RootDeserialize(rootMap)
	}
	Merge2Root(root, ReadLogLines(common.LogFileName))
	// Connect Shadow master
	addr := viper.GetString(common.SMAddr) + viper.GetString(common.SMPort)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	errCount := 0
	for err != nil && errCount < maxErrorCount {
		conn, err = grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		errCount++
	}
	if errCount < maxErrorCount {
		logrus.Info("Success Connect to Shadow master!")
	}
	GlobalMasterHandler = &MasterHandler{
		ClientCon: conn,
		FSM:       &MasterFSM{},
	}
	GlobalMasterHandler.EtcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndPoint},
		DialTimeout: 5 * time.Second,
	})
	err = GlobalMasterHandler.initRaft()
	if err != nil {
		logrus.Panicf("fail to init raft, error detail : %s", err.Error())
	}
}

func (handler *MasterHandler) initRaft() error {
	logrus.Infof("begin")
	raftConfig := raft.DefaultConfig()
	raftConfig.Logger = hclog.L()

	localIP, err := util.GetLocalIP()
	if err != nil {
		return err
	}
	handler.raftAddress = localIP + common.AddressDelimiter + raftPort
	raftConfig.LocalID = raft.ServerID(handler.raftAddress)

	handler.MonitorChan = make(chan bool, 1)
	raftConfig.NotifyCh = handler.MonitorChan
	go monitorCluster()

	addr, err := net.ResolveTCPAddr(tcp, handler.raftAddress)
	if err != nil {
		logrus.Errorf("1")
		return err
	}
	transport, err := raft.NewTCPTransport(handler.raftAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		logrus.Errorf("2")
		return err
	}
	logrus.Infof("second")

	ss, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		return err
	}
	logrus.Infof("third")

	// boltDB implement log store and stable store interface
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		logrus.Errorf("eeeee")
		return err
	}
	logrus.Infof("third")

	// raft system
	r, err := raft.NewRaft(raftConfig, handler.FSM, boltDB, boltDB, ss, transport)
	if err != nil {
		return err
	}
	handler.Raft = r
	logrus.Infof("third")
	ctx := context.Background()
	kv := clientv3.NewKV(GlobalMasterHandler.EtcdClient)
	getResp, err := kv.Get(ctx, etcdKey)
	if err != nil {
		logrus.Errorf("fail to get kv when init")
	}

	if len(getResp.Kvs) == 0 {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		handler.Raft.BootstrapCluster(configuration)
		_, _ = kv.Put(ctx, etcdKey, handler.raftAddress)
		//todo 失败重试
	} else {
		_, err := handler.join2Cluster(&pb.Join2ClusterArgs{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (handler *MasterHandler) join2Cluster(args *pb.Join2ClusterArgs) (*pb.Join2ClusterReply, error) {
	ctx := context.Background()
	kv := clientv3.NewKV(GlobalMasterHandler.EtcdClient)
	getResp, err := kv.Get(ctx, etcdKey)
	if err != nil {
		logrus.Errorf("fail to get kv when join cluster")
	}
	addr := string(getResp.Kvs[0].Value)
	addr = strings.Split(addr, common.AddressDelimiter)[0] + common.AddressDelimiter + masterPort
	logrus.Infof("leader address : %s", addr)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewRaftServiceClient(conn)
	reply, err := client.Join2Cluster(ctx, args)
	if err != nil {
		logrus.Errorf("fail to join cluster, error detail : %s", err.Error())
	}
	return reply, err
}

// Join2Cluster 由Chunkserver调用该方法，将对应DataNode注册到本NameNode上
func (handler *MasterHandler) Join2Cluster(ctx context.Context, args *pb.Join2ClusterArgs) (*pb.Join2ClusterReply, error) {
	logrus.Info("get request")
	p, _ := peer.FromContext(ctx)
	address := strings.Split(p.Addr.String(), common.AddressDelimiter)[0]
	rAddress := address + common.AddressDelimiter + raftPort
	logrus.Infof("get request to join cluster, address : %s", rAddress)
	cf := handler.Raft.GetConfiguration()

	if err := cf.Error(); err != nil {
		logrus.Errorf("fail to get config, error detail : %s", err.Error())
		return nil, err
	}

	for _, server := range cf.Configuration().Servers {
		if server.ID == raft.ServerID(rAddress) {
			logrus.Errorf("node already joined raft cluster, address : %s", address)
			return nil, fmt.Errorf("node already joined raft cluster, address : %s", address)
		}
	}

	f := handler.Raft.AddVoter(raft.ServerID(rAddress), raft.ServerAddress(rAddress), 0, 0)
	if err := f.Error(); err != nil {
		logrus.Errorf("fail to add voter, address : %s, error deatail : %s", address, err.Error())
		return nil, err
	}

	logrus.Infof("node joined successfully, address : %s", address)
	return &pb.Join2ClusterReply{}, nil
}

func monitorCluster() {
	for {
		select {
		case isLeader := <-GlobalMasterHandler.MonitorChan:
			if isLeader {
				kv := clientv3.NewKV(GlobalMasterHandler.EtcdClient)
				_, err := kv.Put(context.Background(), etcdKey, GlobalMasterHandler.raftAddress)
				if err != nil {
					logrus.Errorf("fail to put kv when leader change")
				}
				logrus.Infof("become leader, change etcd leader infomation")
			} else {
				logrus.Infof("become follower")
			}
		}
	}
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
	//client := pb.NewSendOperationServiceClient(handler.ClientCon)
	//op := OperationMkdir(args.Path, args.DirName)
	//_, err := client.SendOperation(context.Background(), op)
	//if err != nil {
	//	logrus.Errorf("Fail to store opertion in the file, error code: %v, error detail: %s,", common.MasterCheckAndMkdirFailed, err.Error())
	//	details, _ := status.New(codes.Unknown, err.Error()).WithDetails(&pb.RPCError{
	//		Code: common.MasterCheckAndMkdirFailed,
	//		Msg:  err.Error(),
	//	})
	//	return nil, details.Err()
	//}

	operation := &MkdirOperation{
		Id:       util.GenerateUUIDString(),
		Des:      args.Path,
		FileName: args.DirName,
		Type:     Operation_Mkdir,
	}
	operationBytes, err := json.Marshal(operation)
	if err != nil {
		return nil, err
	}

	applyFuture := handler.Raft.Apply(operationBytes, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		logrus.Errorf("fail to apply, error detail : %s", err.Error())
		logrus.Errorf("Fail to check args and make directory at target path, error code: %v, error detail: %s,", common.MasterCheckAndMkdirFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndMkdirFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	//err = DoCheckAndMkdir(args.Path, args.DirName)
	//if err != nil {
	//	logrus.Errorf("Fail to check args and make directory at target path, error code: %v, error detail: %s,", common.MasterCheckAndMkdirFailed, err.Error())
	//	details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
	//		Code: common.MasterCheckAndMkdirFailed,
	//		Msg:  err.Error(),
	//	})
	//	return nil, details.Err()
	//}

	//client = pb.NewSendOperationServiceClient(handler.ClientCon)
	//_, err = client.FinishOperation(context.Background(), &pb.OperationArgs{
	//	Uuid:     op.Uuid,
	//	IsFinish: true,
	//})
	//if err != nil {
	//	logrus.Errorf("Finish Opeartion Failed, error code: %v, error detail: %s,", common.MasterCheckAndMkdirFailed, err.Error())
	//	details, _ := status.New(codes.Unknown, err.Error()).WithDetails(&pb.RPCError{
	//		Code: common.MasterCheckAndMkdirFailed,
	//		Msg:  err.Error(),
	//	})
	//	return nil, details.Err()
	//}

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
