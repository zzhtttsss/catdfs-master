package internal

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"tinydfs-base/protocol/pb"
	"tinydfs-master/internal"
)

var SM *ShadowMaster

func TestMain(m *testing.M) {
	SM = CreateShadowMaster()
	m.Run()
	SM = nil
}

func SetupAndTeardownTestCase() func(t *testing.T) {
	_ = os.Remove(LogFileName)
	_ = os.Remove(DirectoryFileName)
	return func(t *testing.T) {}
}

func TestLogger_Info(t *testing.T) {
	teardown := SetupAndTeardownTestCase()
	defer teardown(t)
	test := map[string]*struct {
		op *pb.OperationArgs
	}{
		"Add": {
			op: internal.OperationAdd("/a/", true, "b.txt", 2024),
		},
		"Remove": {
			op: internal.OperationRemove("/a/b.txt"),
		},
		"Rename": {
			op: internal.OperationRename("/a/b.txt", "d.txt"),
		},
		"Move": {
			op: internal.OperationMove("/a/b.txt", "/c/d.txt"),
		},
	}
	for _, c := range test {
		_ = SM.Info(c.op)
	}
	stat, err := os.Stat(LogFileName)
	assert.Nil(t, err)
	assert.NotNil(t, stat)
}

func TestReadLogLines(t *testing.T) {
	test := []string{"Add", "Remove", "Rename", "Move"}
	res := SM.readLogLines(LogFileName)
	defer func() {
		_ = os.Remove(".\\edits.txt")
	}()
	for i, r := range res {
		assert.Equal(t, test[i], r.Type)
	}
}

func TestRootSerialize(t *testing.T) {
	_ = os.Remove(internal.RootFileName)
	SM.RootSerialize()
}

func TestReadRootLines(t *testing.T) {
	_ = os.Remove(internal.RootFileName)
	//internal.initRoot("/a/b/c.txt")
	SM.RootSerialize()
	res := SM.readRootLines()
	assert.Equal(t, 4, len(res))
	for id, n := range res {
		assert.Equal(t, id, n.Id)
	}
}

func TestRootUnSerialize(t *testing.T) {
	_ = os.Remove(internal.RootFileName)
	//internal.initRoot("/a/b/c.txt")
	SM.RootSerialize()
	SM.RootDeserialize(SM.readRootLines())
	assert.NotNil(t, SM.shadowRoot)
	assert.Equal(t, 1, len(SM.shadowRoot.ChildNodes))
	child1, ok := SM.shadowRoot.ChildNodes["a"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(child1.ChildNodes))
}
