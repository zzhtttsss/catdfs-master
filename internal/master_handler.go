package internal

import (
	"context"
	"encoding/json"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
	"tinydfs-base/config"
	"tinydfs-base/util"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

const (
	ttl = 5
)

var GlobalMasterHandler *MasterHandler
var Logger *logrus.Logger

func init() {
	config.InitConfig()
	Logger = config.InitLogger(Logger, true)
	Logger.SetLevel(logrus.Level(viper.GetInt(common.MasterLogLevel)))

}

// MasterHandler represent a master node to handle all incoming requests.
type MasterHandler struct {
	ClientCon *grpc.ClientConn
	// FSM is used to ensure metadata consistency.
	FSM *MasterFSM
	// Raft manage connection with the cluster.
	Raft *raft.Raft
	// FollowerStateObserver is used to observer the change of follower state.
	FollowerStateObserver *raft.Observer
	// MonitorChan contain the change of current master's state.
	MonitorChan chan bool
	// EtcdClient is used to get connect with ETCD.
	EtcdClient *clientv3.Client
	// FollowerLeaseId is lease id when this node is a follower.
	FollowerLeaseId clientv3.LeaseID
	// raftAddress is the address for communication with cluster nodes.
	raftAddress string
	// SelfAddr represents the local address
	SelfAddr string
	pb.UnimplementedRegisterServiceServer
	pb.UnimplementedHeartbeatServiceServer
	pb.UnimplementedMasterAddServiceServer
	pb.UnimplementedMasterMkdirServiceServer
	pb.UnimplementedMasterMoveServiceServer
	pb.UnimplementedMasterRemoveServiceServer
	pb.UnimplementedMasterRenameServiceServer
	pb.UnimplementedMasterListServiceServer
	pb.UnimplementedMasterStatServiceServer
	pb.UnimplementedMasterGetServiceServer
	pb.UnimplementedRaftServiceServer
}

// CreateMasterHandler create a global MasterHandler.
func CreateMasterHandler() {
	var err error
	GlobalMasterHandler = &MasterHandler{
		FSM: &MasterFSM{},
	}
	GlobalMasterHandler.EtcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{viper.GetString(common.EtcdEndPoint)},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		Logger.Panicf("Fail to get etcd client, error detail : %s", err.Error())
	}
	err = GlobalMasterHandler.initRaft()
	if err != nil {
		Logger.Panicf("Fail to init raft, error detail : %s", err.Error())
	}
}

// initRaft initials the raft config of the MasterHandler.
func (handler *MasterHandler) initRaft() error {
	raftConfig := raft.DefaultConfig()
	raftConfig.SnapshotInterval = 20 * time.Second
	//raftConfig.SnapshotThreshold = 128
	raftConfig.Logger = hclog.L()

	localIP, err := util.GetLocalIP()
	if err != nil {
		return err
	}
	handler.raftAddress = localIP + common.AddressDelimiter + viper.GetString(common.MasterRaftPort)
	raftConfig.LocalID = raft.ServerID(handler.raftAddress)

	handler.MonitorChan = make(chan bool, 1)
	raftConfig.NotifyCh = handler.MonitorChan
	ctx := context.Background()
	go handler.monitorCluster(ctx)

	addr, err := net.ResolveTCPAddr(common.TCP, handler.raftAddress)
	if err != nil {
		Logger.Errorf("Fail to resolve TCP address, error detail: %s", err.Error())
		return err
	}
	tcpTransport, err := raft.NewTCPTransport(handler.raftAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		Logger.Errorf("Fail to create TCP Transport, error detail: %s", err.Error())

		return err
	}

	raftDir := viper.GetString(common.MasterRaftDir)
	fss, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		Logger.Errorf("Fail to create file snapshot store, error detail: %s", err.Error())
		return err
	}
	logDB, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, common.LogDBName))
	if err != nil {
		Logger.Errorf("Fail to create log DB, error detail: %s", err.Error())
		return err
	}
	stableDB, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, common.StableDBName))
	if err != nil {
		Logger.Errorf("Fail to create stable DB, error detail: %s", err.Error())
		return err
	}

	r, err := raft.NewRaft(raftConfig, handler.FSM, logDB, stableDB, fss, tcpTransport)
	if err != nil {
		Logger.Errorf("Fail to create raft node, error detail: %s", err.Error())
		return err
	}
	handler.Raft = r
	err = handler.bootstrapOrJoinCluster()
	if err != nil {
		Logger.Errorf("Fail to bootstrap or join, error detail: %s", err.Error())
		return err
	}
	return nil
}

// bootstrapOrJoinCluster bootstraps cluster when there is no cluster, otherwise
// join cluster.
func (handler *MasterHandler) bootstrapOrJoinCluster() error {
	ctx := context.Background()
	kv := clientv3.NewKV(handler.EtcdClient)
	getResp, err := kv.Get(ctx, common.LeaderAddressKey)
	if err != nil {
		Logger.Errorf("Fail to get leader address from etcd when init, error detail: %s", err.Error())
		return err
	}
	if len(getResp.Kvs) == 0 {
		Logger.Infof("No existing cluster, start to bootstrap cluster.")
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
			Logger.Errorf("Fail to bootstrap, error detail: %s", err.Error())
		}
		_, err = kv.Put(ctx, common.LeaderAddressKey, handler.raftAddress)
		if err != nil {
			Logger.Errorf("Fail to put leader address into etcd when init, error detail: %s", err.Error())
			//todo 失败重试
		}
	} else {
		Logger.Infof("Already have cluster, start to join cluster.")
		_, err := handler.joinCluster(getResp)
		if err != nil {
			Logger.Errorf("Fail to join cluster, error detail: %s", err.Error())
			return err
		}
	}
	return nil
}

// joinCluster joins follower itself to the cluster.
func (handler *MasterHandler) joinCluster(getResp *clientv3.GetResponse) (*pb.JoinClusterReply, error) {
	ctx := context.Background()
	addr := string(getResp.Kvs[0].Value)
	addr = strings.Split(addr, common.AddressDelimiter)[0] + viper.GetString(common.MasterPort)

	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := pb.NewRaftServiceClient(conn)
	reply, err := client.JoinCluster(ctx, &pb.JoinClusterArgs{})
	if err != nil {
		Logger.Errorf("Fail to join cluster, error detail : %s", err.Error())
	}
	go handler.putAndKeepFollower()
	return reply, err
}

// putAndKeepFollower registers follower to etcd and create a lease to keep alive.
func (handler *MasterHandler) putAndKeepFollower() {
	client := handler.EtcdClient
	ctx := context.Background()
	kv := clientv3.NewKV(client)
	hostname, err := os.Hostname()
	if err != nil {
		Logger.Panicf("Fail to get hostname, error detail: %s", err.Error())
	}
	lease, err := client.Grant(ctx, int64(ttl))
	if err != nil {
		Logger.Panicf("Fail to create lease with etcd, error detail: %s", err.Error())
	}
	handler.FollowerLeaseId = lease.ID
	_, err = kv.Put(ctx, common.FollowerKeyPrefix+hostname, handler.raftAddress, clientv3.WithLease(lease.ID))

	keepAliveChan, err := client.KeepAlive(ctx, lease.ID)
	if err != nil {
		Logger.Panicf("Fail to keep lease alive, error detail: %s", err.Error())
	}
	for res := range keepAliveChan {
		b, _ := json.Marshal(res)
		Logger.Debugf("Success to keep lease alive: %s", string(b))
	}
	Logger.Infof("Stop keeping lease alive.")
}

// JoinCluster is called by follower. Leader joins a follower to the cluster.
func (handler *MasterHandler) JoinCluster(ctx context.Context, args *pb.JoinClusterArgs) (*pb.JoinClusterReply, error) {
	p, _ := peer.FromContext(ctx)
	address := strings.Split(p.Addr.String(), common.AddressDelimiter)[0]
	rAddress := address + common.AddressDelimiter + viper.GetString(common.MasterRaftPort)
	Logger.Infof("Get request to join cluster, address : %s", rAddress)
	cf := handler.Raft.GetConfiguration()

	if err := cf.Error(); err != nil {
		Logger.Errorf("Fail to get raft config, error detail : %s", err.Error())
		return nil, err
	}

	for _, server := range cf.Configuration().Servers {
		if server.ID == raft.ServerID(rAddress) {
			Logger.Errorf("Node already joined raft cluster, address : %s", address)
			return nil, fmt.Errorf("node already joined raft cluster, address : %s", address)
		}
	}

	f := handler.Raft.AddVoter(raft.ServerID(rAddress), raft.ServerAddress(rAddress), 0, 0)
	if err := f.Error(); err != nil {
		Logger.Errorf("Fail to add voter, address : %s, error deatail : %s", address, err.Error())
		return nil, err
	}

	Logger.Infof("Node joined successfully, address : %s", address)
	return &pb.JoinClusterReply{}, nil
}

// monitorCluster run in a goroutine.
// This function will monitor the change of current master's state (leader ->
// follower, follower -> leader).
// When current master become the leader of the cluster, it will:
// 1. change leader address in etcd.
// 2. deregister as follower from etcd.
// 3. register an observer to observe follower state.
// 4. monitor heartbeat of all DataNode in a goroutine.
// When current master become the follower of the cluster, it will:
// 1. deregister the observer which observes follower state.
// 2. use cancel function to stop the goroutine which monitors heartbeat.
// 3. register itself to etcd as follower.
func (handler *MasterHandler) monitorCluster(ctx context.Context) {
	for {
		subContext, cancel := context.WithCancel(ctx)
		select {
		case isLeader := <-handler.MonitorChan:
			if isLeader {
				// todo 错误处理
				kv := clientv3.NewKV(handler.EtcdClient)
				_, err := kv.Put(ctx, common.LeaderAddressKey, handler.raftAddress)
				if err != nil {
					Logger.WithContext(ctx).Errorf("Fail to put kv when leader change, error detail: %s", err.Error())
				}
				_, err = handler.EtcdClient.Revoke(ctx, handler.FollowerLeaseId)
				if err != nil {
					Logger.WithContext(ctx).Errorf("Fail to revoke lease, error detail: %s", err.Error())
				}
				handler.FollowerStateObserver = getFollowerStateObserver()
				handler.Raft.RegisterObserver(handler.FollowerStateObserver)
				StartMonitor(subContext)
				Logger.WithContext(ctx).Infof("Become leader, success to change etcd leader infomation and monitor datanodes")
			} else {
				handler.Raft.DeregisterObserver(handler.FollowerStateObserver)
				cancel()
				go handler.putAndKeepFollower()
				Logger.WithContext(ctx).Infof("Become follower, keep lease with etcd.")
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
			Logger.Infof("Remove a dead follower, address: %s", string(ob.PeerID))
		}
		return ok
	}
	return raft.NewObserver(observerChan, false, filterFn)
}

// Register is called by chunkserver. It registers a DataNode to master.
func (handler *MasterHandler) Register(ctx context.Context, args *pb.DNRegisterArgs) (*pb.DNRegisterReply, error) {
	p, _ := peer.FromContext(ctx)
	address := strings.Split(p.Addr.String(), ":")[0]
	Logger.WithContext(ctx).Infof("Get request for registering a datanode, address: %s", address)
	need2Expand := IsNeed2Expand(int(args.UsedCapacity), int(args.FullCapacity))
	dataNodeId := util.GenerateUUIDString()
	// register first
	operation := &RegisterOperation{
		Id:           util.GenerateUUIDString(),
		Address:      address,
		DataNodeId:   dataNodeId,
		ChunkIds:     args.ChunkIds,
		FullCapacity: int(args.FullCapacity),
		UsedCapacity: int(args.UsedCapacity),
		IsNeedExpand: need2Expand,
	}
	data := getData4Apply(operation, common.OperationRegister)
	applyFuture := handler.Raft.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		Logger.Errorf("Fail to register, error code: %v, error detail: %s,", common.MasterRegisterFailed, err.Error())
		details, _ := status.New(codes.Internal, "").WithDetails(&pb.RPCError{
			Code: common.MasterRegisterFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	response := (applyFuture.Response()).(*ApplyResponse)
	if err := response.Error; err != nil {
		Logger.Errorf("Fail to register, error code: %v, error detail: %s,", common.MasterRegisterFailed, err.Error())
		details, _ := status.New(codes.Internal, "").WithDetails(&pb.RPCError{
			Code: common.MasterRegisterFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	// At that time, the new datanode has registered
	pendingCount := 0
	if need2Expand {
		pendingCount = DoExpand(GetDataNode(dataNodeId))
	}
	id := response.Response.(string)
	rep := &pb.DNRegisterReply{
		Id:           id,
		PendingCount: uint32(pendingCount + len(args.ChunkIds)),
	}
	Logger.WithContext(ctx).Infof("Success to register a datanode, address: %s, id: %s, isNeedToExpand: %v",
		address, id, need2Expand)
	csCountMonitor.Inc()
	return rep, nil
}

// Heartbeat is called by chunkserver. It sets the last heartbeat time to now time to
// maintain the connection between chunkserver and master.
func (handler *MasterHandler) Heartbeat(ctx context.Context, args *pb.HeartbeatArgs) (*pb.HeartbeatReply, error) {
	Logger.WithContext(ctx).Debugf("Get heartbeat, datanodeId: %s, isReady: %v", args.Id, args.IsReady)
	successInfos := ConvChunkInfo(args.SuccessChunkInfos)
	failInfos := ConvChunkInfo(args.FailChunkInfos)
	operation := &HeartbeatOperation{
		Id:           util.GenerateUUIDString(),
		DataNodeId:   args.Id,
		ChunkIds:     args.ChunkId,
		IOLoad:       args.IOLoad,
		FullCapacity: args.FullCapacity,
		UsedCapacity: args.UsedCapacity,
		SuccessInfos: successInfos,
		FailInfos:    failInfos,
		IsReady:      args.IsReady,
	}
	data := getData4Apply(operation, common.OperationHeartbeat)
	applyFuture := handler.Raft.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		Logger.Errorf("Fail to heartbeat, error code: %v, error detail: %s,", common.MasterHeartbeatFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterHeartbeatFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	response := (applyFuture.Response()).(*ApplyResponse)
	if err := response.Error; err != nil {
		Logger.Errorf("Fail to heartbeat, error code: %v, error detail: %s,", common.MasterHeartbeatFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterHeartbeatFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	chunkSendInfos := (response.Response).([]ChunkSendInfo)
	nextChunkInfos := DeConvChunkInfo(chunkSendInfos)
	dataNodeAddress := GetDataNodeAddresses(chunkSendInfos)
	heartbeatReply := &pb.HeartbeatReply{
		DataNodeAddress: dataNodeAddress,
		ChunkInfos:      nextChunkInfos,
	}
	return heartbeatReply, nil
}

// CheckArgs4Add is called by client. It checks whether there is enough space
// to store the file and the path and file name entered by the user in the Add
// operation are legal.
func (handler *MasterHandler) CheckArgs4Add(ctx context.Context, args *pb.CheckArgs4AddArgs) (*pb.CheckArgs4AddReply, error) {
	Logger.WithContext(ctx).Infof("Get request for checking path and filename for add operation, path: %s, filename: %s, size: %d", args.Path, args.FileName, args.Size)
	RequestCountInc(handler.SelfAddr, common.OperationAdd)
	if int(StorableNum.Load()) < viper.GetInt(common.ReplicaNum) {
		err := fmt.Errorf("insufficient free space in the catfs to store the file")
		Logger.Errorf("Fail to check path and filename for add operation, error code: %v, error detail: %s,", common.MasterCheckArgs4AddFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckArgs4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	operation := &AddOperation{
		Id:         util.GenerateUUIDString(),
		FileNodeId: util.GenerateUUIDString(),
		Path:       args.Path,
		FileName:   args.FileName,
		Size:       args.Size,
		Stage:      common.CheckArgs,
	}
	data := getData4Apply(operation, common.OperationAdd)
	applyFuture := handler.Raft.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		Logger.Errorf("Fail to check path and filename for add operation, error code: %v, error detail: %s,", common.MasterCheckArgs4AddFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckArgs4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	response := (applyFuture.Response()).(*ApplyResponse)
	if err := response.Error; err != nil {
		Logger.Errorf("Fail to check path and filename for add operation, error code: %v, error detail: %s,", common.MasterCheckArgs4AddFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckArgs4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	Logger.WithContext(ctx).Infof("Success to check path and filename for add operation, path: %s, filename: %s, size: %d", args.Path, args.FileName, args.Size)
	return (response.Response).(*pb.CheckArgs4AddReply), nil

}

// CheckAndGet is called by client, It checks get args and gets the FileNode
// according to path.
func (handler *MasterHandler) CheckAndGet(ctx context.Context, args *pb.CheckAndGetArgs) (*pb.CheckAndGetReply, error) {
	Logger.WithContext(ctx).Infof("Get request for checking path for get operation, Path: %s", args.Path)
	RequestCountInc(handler.SelfAddr, common.OperationGet)
	operation := &GetOperation{
		Id:    util.GenerateUUIDString(),
		Path:  args.Path,
		Stage: common.CheckArgs,
	}
	data := getData4Apply(operation, common.OperationGet)
	applyFuture := handler.Raft.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		Logger.Errorf("Fail to check path for get operation, error code: %v, error detail: %s", common.MasterCheckAndGetFailed, err)
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndGetFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	response := (applyFuture.Response()).(*ApplyResponse)
	if err := response.Error; err != nil {
		Logger.Errorf("Fail tocheck path for get operation, error code: %v, error detail: %s", common.MasterCheckAndGetFailed, err)
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndGetFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	fileNode := response.Response.(*FileNode)
	rep := &pb.CheckAndGetReply{
		FileNodeId:  fileNode.Id,
		ChunkNum:    int32(len(fileNode.Chunks)),
		OperationId: operation.Id,
		FileSize:    fileNode.Size,
	}
	Logger.WithContext(ctx).Infof("Success to check path for get operation, path: %s", args.Path)
	return rep, nil
}

// GetDataNodes4Add is called by client. It allocates DataNode for a batch of Chunk.
func (handler *MasterHandler) GetDataNodes4Add(ctx context.Context, args *pb.GetDataNodes4AddArgs) (*pb.GetDataNodes4AddReply, error) {
	Logger.WithContext(ctx).Infof("Get request for getting dataNodes for single chunk, FileNodeId: %s, ChunkNum: %d", args.FileNodeId, args.ChunkNum)
	operation := &AddOperation{
		Id:         util.GenerateUUIDString(),
		FileNodeId: args.FileNodeId,
		ChunkNum:   args.ChunkNum,
		Stage:      common.GetDataNodes,
	}
	data := getData4Apply(operation, common.OperationAdd)
	applyFuture := handler.Raft.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		Logger.Errorf("Fail to get dataNodes for single chunk for add operation, error code: %v, error detail: %s,", common.MasterGetDataNodes4AddFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterGetDataNodes4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	response := (applyFuture.Response()).(*ApplyResponse)
	if err := response.Error; err != nil {
		Logger.Errorf("Fail to get dataNodes for single chunk for add operation, error code: %v, error detail: %s,", common.MasterGetDataNodes4AddFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterGetDataNodes4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	Logger.WithContext(ctx).Infof("Success to get dataNodes for single chunk, FileNodeId: %s, ChunkNum: %d", args.FileNodeId, args.ChunkNum)
	return (response.Response).(*pb.GetDataNodes4AddReply), nil
}

// GetDataNodes4Get is called by client. It finds the dataNodes for the specified ChunkId.
func (handler *MasterHandler) GetDataNodes4Get(ctx context.Context, args *pb.GetDataNodes4GetArgs) (*pb.GetDataNodes4GetReply, error) {
	Logger.WithContext(ctx).Infof("Get request for getting DataNodes, FileNodeId: %s", args.FileNodeId)
	operation := &GetOperation{
		Id:         util.GenerateUUIDString(),
		FileNodeId: args.FileNodeId,
		ChunkIndex: args.ChunkIndex,
		Stage:      common.GetDataNodes,
	}
	data := getData4Apply(operation, common.OperationGet)
	applyFuture := handler.Raft.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		Logger.Errorf("Fail to get DataNodes for get operation, error code: %v, error detail: %s,", common.MasterGetDataNodes4GetFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterGetDataNodes4GetFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	response := (applyFuture.Response()).(*ApplyResponse)
	if err := response.Error; err != nil {
		Logger.Errorf("Fail to get DataNodes for get operation, error code: %v, error detail: %s,", common.MasterGetDataNodes4GetFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterGetDataNodes4GetFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	Logger.WithContext(ctx).Infof("Success to get DataNodes for get operation, FileNodeId: %s, ChunkIndex: %d", args.FileNodeId, args.ChunkIndex)
	return (response.Response).(*pb.GetDataNodes4GetReply), nil
}

// Callback4Add is called by client. It handles the result of the add operation.
func (handler *MasterHandler) Callback4Add(ctx context.Context, args *pb.Callback4AddArgs) (*pb.Callback4AddReply, error) {
	Logger.WithContext(ctx).Infof("Get the result of add operation, FileNodeId: %s", args.FileNodeId)
	operation := &AddOperation{
		Id:           util.GenerateUUIDString(),
		FileNodeId:   args.FileNodeId,
		FailChunkIds: args.FailChunkIds,
		Path:         args.FilePath,
		Stage:        common.UnlockDic,
	}
	infos := make([]util.ChunkSendResult, len(args.Infos))
	for i, info := range args.Infos {
		infos[i] = util.ChunkSendResult{
			ChunkId:          info.ChunkId,
			SuccessDataNodes: info.SuccessNode,
			FailDataNodes:    info.FailNode,
		}
	}
	operation.Infos = infos
	data := getData4Apply(operation, common.OperationAdd)
	applyFuture := handler.Raft.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		Logger.Errorf("Fail to handle the result of add operation, error code: %v, error detail: %s,", common.MasterCallback4AddFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCallback4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	response := (applyFuture.Response()).(*ApplyResponse)
	if err := response.Error; err != nil {
		Logger.Errorf("Fail to handle the result of add operation, error code: %v, error detail: %s,", common.MasterCallback4AddFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCallback4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	rep := &pb.Callback4AddReply{}
	Logger.WithContext(ctx).Infof("Success to handle the result of add operation, FileNodeId: %s", args.FileNodeId)
	SuccessCountInc(handler.SelfAddr, common.OperationAdd)
	return rep, nil
}

// CheckAndMkdir is called by client. It checks args and makes directory at target path.
func (handler *MasterHandler) CheckAndMkdir(ctx context.Context, args *pb.CheckAndMkDirArgs) (*pb.CheckAndMkDirReply, error) {
	Logger.WithContext(ctx).Infof("Get request for making directory at target path, path: %s, dirName: %s", args.Path, args.DirName)
	RequestCountInc(handler.SelfAddr, common.OperationMkdir)
	operation := &MkdirOperation{
		Id:       util.GenerateUUIDString(),
		Path:     args.Path,
		FileName: args.DirName,
	}
	data := getData4Apply(operation, common.OperationMkdir)
	applyFuture := handler.Raft.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		Logger.Errorf("Fail to make directory at target path, error code: %v, error detail: %s,", common.MasterCheckAndMkdirFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndMkdirFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	response := (applyFuture.Response()).(*ApplyResponse)
	if err := response.Error; err != nil {
		Logger.Errorf("Fail to make directory at target path, error code: %v, error detail: %s,", common.MasterCheckAndMkdirFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndMkdirFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	rep := &pb.CheckAndMkDirReply{}
	Logger.WithContext(ctx).Infof("Success to make directory at target path, path: %s, dirName: %s", args.Path, args.DirName)
	SuccessCountInc(handler.SelfAddr, common.OperationMkdir)
	return rep, nil
}

// CheckAndMove is called by client. It checks args and moves directory or file
// to target path.
func (handler *MasterHandler) CheckAndMove(ctx context.Context, args *pb.CheckAndMoveArgs) (*pb.CheckAndMoveReply, error) {
	Logger.WithContext(ctx).Infof("Get request for moving directory or file to target path, sourcePath: %s, targetPath: %s", args.SourcePath, args.TargetPath)
	RequestCountInc(handler.SelfAddr, common.OperationMove)
	operation := &MoveOperation{
		Id:         util.GenerateUUIDString(),
		SourcePath: args.SourcePath,
		TargetPath: args.TargetPath,
	}
	data := getData4Apply(operation, common.OperationMove)
	applyFuture := handler.Raft.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		Logger.Errorf("Fail to move directory or file to target path, error code: %v, error detail: %s,", common.MasterCheckAndMoveFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndMoveFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	response := (applyFuture.Response()).(*ApplyResponse)
	if err := response.Error; err != nil {
		Logger.Errorf("Fail to move directory or file to target path, error code: %v, error detail: %s,", common.MasterCheckAndMoveFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndMoveFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	rep := &pb.CheckAndMoveReply{}
	Logger.WithContext(ctx).Infof("Success to move directory or file to target path, sourcePath: %s, targetPath: %s", args.SourcePath, args.TargetPath)
	SuccessCountInc(handler.SelfAddr, common.OperationMove)
	return rep, nil
}

// CheckAndRemove is called by client. It checks args and removes directory or
// file at target path.
func (handler *MasterHandler) CheckAndRemove(ctx context.Context, args *pb.CheckAndRemoveArgs) (*pb.CheckAndRemoveReply, error) {
	Logger.WithContext(ctx).Infof("Get request for removing directory or file at target path, path: %s", args.Path)
	RequestCountInc(handler.SelfAddr, common.OperationRemove)
	operation := &RemoveOperation{
		Id:   util.GenerateUUIDString(),
		Path: args.Path,
	}
	data := getData4Apply(operation, common.OperationRemove)
	applyFuture := handler.Raft.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		Logger.Errorf("Fail to remove directory or file at target path, error code: %v, error detail: %s,", common.MasterCheckAndRemoveFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndRemoveFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	response := (applyFuture.Response()).(*ApplyResponse)
	if err := response.Error; err != nil {
		Logger.Errorf("Fail to remove directory or file at target path, error code: %v, error detail: %s,", common.MasterCheckAndRemoveFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndRemoveFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	rep := &pb.CheckAndRemoveReply{}
	Logger.WithContext(ctx).Infof("Success to remove directory or file at target path, path: %s", args.Path)
	SuccessCountInc(handler.SelfAddr, common.OperationRemove)
	return rep, nil
}

// CheckAndList is called by client. It checks args and list the specified directory.
func (handler *MasterHandler) CheckAndList(ctx context.Context, args *pb.CheckAndListArgs) (*pb.CheckAndListReply, error) {
	Logger.WithContext(ctx).Infof("Get request for listing the specified directory, path: %s", args.Path)
	var (
		response interface{}
		err      error
	)
	RequestCountInc(handler.SelfAddr, common.OperationList)
	operation := &ListOperation{
		Id:   util.GenerateUUIDString(),
		Path: args.Path,
	}
	if args.IsLatest {
		data := getData4Apply(operation, common.OperationList)
		applyFuture := handler.Raft.Apply(data, 5*time.Second)
		if err := applyFuture.Error(); err != nil {
			Logger.Errorf("Fail to list specified directory, error code: %v, error detail: %s,", common.MasterCheckAndListFailed, err.Error())
			details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
				Code: common.MasterCheckAndListFailed,
				Msg:  err.Error(),
			})
			return nil, details.Err()
		}
		applyResponse := applyFuture.Response().(*ApplyResponse)
		response = applyResponse.Response
		err = applyResponse.Error
	} else {
		response, err = operation.Apply()
	}
	if err != nil {
		Logger.Errorf("Fail to list specified directory, error code: %v, error detail: %s,", common.MasterCheckAndListFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndListFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	rep := &pb.CheckAndListReply{
		Files: response.([]*pb.FileInfo),
	}
	Logger.WithContext(ctx).Infof("Success to list specified directory, path: %s", args.Path)
	SuccessCountInc(handler.SelfAddr, common.OperationList)
	return rep, nil
}

// CheckAndStat is called by client. It checks args and return the specified file info.
func (handler *MasterHandler) CheckAndStat(ctx context.Context, args *pb.CheckAndStatArgs) (*pb.CheckAndStatReply, error) {
	Logger.WithContext(ctx).Infof("Get request for getting the specified file info, path: %s", args.Path)
	var (
		response interface{}
		err      error
	)

	RequestCountInc(handler.SelfAddr, common.OperationStat)
	operation := &StatOperation{
		Id:   util.GenerateUUIDString(),
		Path: args.Path,
	}
	if args.IsLatest {
		data := getData4Apply(operation, common.OperationStat)
		applyFuture := handler.Raft.Apply(data, 5*time.Second)
		if err := applyFuture.Error(); err != nil {
			Logger.Errorf("Fail to get the specified file info, error code: %v, error detail: %s,", common.MasterCheckAndStatFailed, err.Error())
			details, _ := status.New(codes.Unknown, err.Error()).WithDetails(&pb.RPCError{
				Code: common.MasterCheckAndStatFailed,
				Msg:  err.Error(),
			})
			return nil, details.Err()
		}
		applyResponse := applyFuture.Response().(*ApplyResponse)
		response = applyResponse.Response
		err = applyResponse.Error
	} else {
		response, err = operation.Apply()
	}
	if err != nil {
		Logger.Errorf("Fail to get the specified file info, error code: %v, error detail: %s,", common.MasterCheckAndStatFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndStatFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	info := response.(*FileNode)
	rep := &pb.CheckAndStatReply{
		FileName: info.FileName,
		IsFile:   info.IsFile,
		Size:     info.Size,
	}
	Logger.WithContext(ctx).Infof("Success to get the specified file info, path: %s", args.Path)
	SuccessCountInc(handler.SelfAddr, common.OperationStat)
	return rep, nil
}

// CheckAndRename is called by client. It checks args and renames the specified
// file to a new name.
func (handler *MasterHandler) CheckAndRename(ctx context.Context, args *pb.CheckAndRenameArgs) (*pb.CheckAndRenameReply, error) {
	Logger.WithContext(ctx).Infof("Get request for renaming the specified file to a new name, path: %s, new name: %s", args.Path, args.NewName)
	RequestCountInc(handler.SelfAddr, common.OperationRename)
	operation := &RenameOperation{
		Id:      util.GenerateUUIDString(),
		Path:    args.Path,
		NewName: args.NewName,
	}
	data := getData4Apply(operation, common.OperationRename)
	applyFuture := handler.Raft.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		Logger.Errorf("Fail to rename the specified file to a new name, error code: %v, error detail: %s,",
			common.MasterCheckAndRenameFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndRenameFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	response := (applyFuture.Response()).(*ApplyResponse)
	if err := response.Error; err != nil {
		Logger.Errorf("Fail to rename the specified file to a new name, error code: %v, error detail: %s,",
			common.MasterCheckAndRenameFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckAndRenameFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}

	rep := &pb.CheckAndRenameReply{}
	Logger.WithContext(ctx).Infof("Success to rename the specified file to a new name, path: %s", args.Path)
	SuccessCountInc(handler.SelfAddr, common.OperationRename)
	return rep, nil
}

func (handler *MasterHandler) Server() {
	listener, err := net.Listen(common.TCP, viper.GetString(common.MasterPort))
	if err != nil {
		Logger.Errorf("Fail to server, error code: %v, error detail: %s,", common.MasterRPCServerFailed, err.Error())
		os.Exit(1)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	localIP, _ := util.GetLocalIP()
	handler.SelfAddr = localIP
	pb.RegisterRegisterServiceServer(server, handler)
	pb.RegisterHeartbeatServiceServer(server, handler)
	pb.RegisterMasterAddServiceServer(server, handler)
	pb.RegisterMasterMkdirServiceServer(server, handler)
	pb.RegisterMasterMoveServiceServer(server, handler)
	pb.RegisterMasterRemoveServiceServer(server, handler)
	pb.RegisterMasterListServiceServer(server, handler)
	pb.RegisterMasterRenameServiceServer(server, handler)
	pb.RegisterMasterStatServiceServer(server, handler)
	pb.RegisterMasterGetServiceServer(server, handler)
	pb.RegisterRaftServiceServer(server, handler)
	Logger.Infof("Master is running, listen on %s%s", common.LocalIP, viper.GetString(common.MasterPort))
	server.Serve(listener)
}

// getData4Apply serializes an Operation and encapsulates the result in OpContainer
// and serializes OpContainer again.
func getData4Apply(operation Operation, opType string) []byte {
	operationBytes, _ := json.Marshal(operation)
	opContainer := OpContainer{
		OpType: opType,
		OpData: operationBytes,
	}
	data, _ := json.Marshal(opContainer)
	return data
}
