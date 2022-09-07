package internal

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestLogger_Info(t *testing.T) {
	test := map[string]*struct {
		op *Operation
	}{
		"Add": {
			op: OperationAdd("/a/b.txt", true),
		},
		"Remove": {
			op: OperationRemove("/a/b.txt"),
		},
		"Rename": {
			op: OperationRename("/a/b.txt", "d.txt"),
		},
		"Move": {
			op: OperationMove("/a/b.txt", "/c/d.txt"),
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
	_ = os.Remove(RootFileName)
	initRoot("/a/b/c.txt")
	RootSerialize()
}

func TestReadRootLines(t *testing.T) {
	_ = os.Remove(RootFileName)
	initRoot("/a/b/c.txt")
	RootSerialize()
	res := SM.readRootLines()
	assert.Equal(t, 4, len(res))
	for id, n := range res {
		assert.Equal(t, id, n.Id)
	}
}

func TestRootUnSerialize(t *testing.T) {
	_ = os.Remove(RootFileName)
	initRoot("/a/b/c.txt")
	RootSerialize()
	SM.RootUnSerialize(SM.readRootLines())
	assert.NotNil(t, SM.shadowRoot)
	assert.Equal(t, 1, len(SM.shadowRoot.childNodes))
	child1, ok := SM.shadowRoot.childNodes["a"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(child1.childNodes))
}
