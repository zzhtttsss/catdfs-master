package internal

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"tinydfs-master/internal"
)

func TestLogger_Info(t *testing.T) {
	test := map[string]*struct {
		op *internal.Operation
	}{
		"Add": {
			op: internal.OperationAdd("/a/b.txt", true),
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
		SM.Info(c.op)
	}
}

func TestReadLogLines(t *testing.T) {
	test := []string{"Add", "Remove", "Rename", "Move"}
	res := SM.readLogLines()
	defer func() {
		_ = os.Remove(".\\edits.txt")
	}()
	for i, r := range res {
		assert.Equal(t, test[i], r.Type)
	}
}

func TestRootSerialize(t *testing.T) {
	_ = os.Remove(internal.RootFileName)
	internal.RootSerialize()
}

func TestReadRootLines(t *testing.T) {
	_ = os.Remove(internal.RootFileName)
	//internal.initRoot("/a/b/c.txt")
	internal.RootSerialize()
	res := SM.readRootLines()
	assert.Equal(t, 4, len(res))
	for id, n := range res {
		assert.Equal(t, id, n.Id)
	}
}

func TestRootUnSerialize(t *testing.T) {
	_ = os.Remove(internal.RootFileName)
	//internal.initRoot("/a/b/c.txt")
	internal.RootSerialize()
	SM.RootUnSerialize(SM.readRootLines())
	assert.NotNil(t, SM.shadowRoot)
	assert.Equal(t, 1, len(SM.shadowRoot.ChildNodes))
	child1, ok := SM.shadowRoot.ChildNodes["a"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(child1.ChildNodes))
}
