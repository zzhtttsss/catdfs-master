package internal

import (
	set "github.com/deckarep/golang-set"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestChunk_String(t *testing.T) {
	type args struct {
		c *Chunk
	}
	tests := []struct {
		name       string
		args       args
		Setup      func(t *testing.T)
		wantResult string
		wantErr    error
	}{
		{
			name: "Success",
			args: args{
				c: &Chunk{
					Id:               "chunk1",
					dataNodes:        set.NewSet("dataNode1", "dataNode2"),
					pendingDataNodes: set.NewSet("dataNode3"),
				},
			},
			wantErr:    nil,
			wantResult: "chunk1$[dataNode1 dataNode2]$[dataNode3]\n",
		},
	}

	// 执行测试用例
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.Setup != nil {
				tt.Setup(t)
			}
			result := tt.args.c.String()
			assert.Equal(t, tt.wantResult, result, "Unexpected result.")
		})
	}
}
