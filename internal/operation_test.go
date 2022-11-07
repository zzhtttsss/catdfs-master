package internal

import (
	"fmt"
	"github.com/agiledragon/gomonkey"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"sync"
	"testing"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

func TestWrite(t *testing.T) {
	file, err := os.OpenFile("write.txt", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()
	var wg = &sync.WaitGroup{}
	var word = "HelloWorld\n"
	for i := 0; i < 10; i++ {
		wg.Add(1)
		p := i
		go func(index int) {
			defer wg.Done()
			content := fmt.Sprintf("%v%s", index, word)
			fmt.Printf("Write %d chunk,Content %s", index, content)
			_, err = file.Seek(int64(len(content)*index), 0)
			if err != nil {
				fmt.Println(err)
			}
			_, err = file.WriteString(content)
			if err != nil {
				fmt.Println(err)
			}
		}(p)
	}
	wg.Wait()
	fmt.Println("Done")
}

func TestRead(t *testing.T) {
	file, _ := os.Open("write.txt")
	for i := 0; i < 7; i++ {
		bytes := make([]byte, 119)
		n, err := file.Read(bytes)
		if err == io.EOF {
			fmt.Println(err)
		}
		fmt.Println(string(bytes[:n]))
	}

}

func TestAddOperation_Apply(t *testing.T) {
	type args struct {
		o AddOperation
	}
	tests := []struct {
		name       string
		args       args
		Setup      func(t *testing.T)
		wantResult interface{}
		wantErr    error
	}{
		{
			name: "CheckArgs_Success",
			args: args{
				o: AddOperation{
					Id:    "op1",
					Size:  120 * common.MB,
					Stage: common.CheckArgs,
				},
			},
			Setup: func(t *testing.T) {
				addFileNode := gomonkey.ApplyFunc(AddFileNode, func(_ string, _ string, _ string,
					_ int64, _ bool) (*FileNode, error) {
					return &FileNode{
						Id:     "fileNode1",
						Chunks: []string{"chunk1", "chunk2"},
					}, nil
				})
				t.Cleanup(func() {
					addFileNode.Reset()
				})
			},
			wantErr: nil,
			wantResult: &pb.CheckArgs4AddReply{
				FileNodeId: "fileNode1",
				ChunkNum:   2,
			},
		},
		{
			name: "GetDataNodes_Success",
			args: args{
				o: AddOperation{
					ChunkNum: 2,
					Stage:    common.GetDataNodes,
				},
			},
			Setup: func(t *testing.T) {
				batchAllocateDataNodes := gomonkey.ApplyFunc(BatchAllocateDataNodes, func(_ int) [][]*DataNode {
					return [][]*DataNode{
						{
							&DataNode{
								Id:      "dataNode11",
								Address: "address11",
							},
							&DataNode{
								Id:      "dataNode12",
								Address: "address12",
							},
							&DataNode{
								Id:      "dataNode13",
								Address: "address13",
							},
						},
						{
							&DataNode{
								Id:      "dataNode21",
								Address: "address21",
							},
							&DataNode{
								Id:      "dataNode22",
								Address: "address22",
							},
							&DataNode{
								Id:      "dataNode23",
								Address: "address23",
							},
						},
					}
				})
				batchAddChunk := gomonkey.ApplyFunc(BatchAddChunk, func([]*Chunk) {
				})
				t.Cleanup(func() {
					batchAllocateDataNodes.Reset()
					batchAddChunk.Reset()
				})
			},
			wantErr: nil,
			wantResult: &pb.GetDataNodes4AddReply{
				DataNodeIds: []*pb.GetDataNodes4AddReply_Array{
					{
						Items: []string{"dataNode11", "dataNode12", "dataNode13"},
					},
					{
						Items: []string{"dataNode21", "dataNode22", "dataNode23"},
					},
				},
				DataNodeAdds: []*pb.GetDataNodes4AddReply_Array{
					{
						Items: []string{"address11", "address12", "address13"},
					},
					{
						Items: []string{"address21", "address22", "address23"},
					},
				},
			},
		},
	}

	// 执行测试用例
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.Setup != nil {
				tt.Setup(t)
			}
			r, err := tt.args.o.Apply()
			assert.Equal(t, tt.wantResult, r, "Unexpected result.")
			if err != nil {
				assert.EqualError(t, err, tt.wantErr.Error(), "Unexpected error.")
			}
		})
	}
}
