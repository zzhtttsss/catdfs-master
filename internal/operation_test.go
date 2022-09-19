package internal

import (
	"container/list"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func createRootFile(rootA *FileNode) func() {
	file, err := os.OpenFile("test.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		fmt.Println("Create error, ", err.Error())
	}
	queue := list.New()
	queue.PushBack(rootA)
	for queue.Len() != 0 {
		cur := queue.Front()
		queue.Remove(cur)
		node, _ := cur.Value.(*FileNode)
		_, _ = file.WriteString(node.String())
		for _, child := range node.ChildNodes {
			queue.PushBack(child)
		}
	}
	_ = file.Close()
	return func() {
		_ = os.Remove(file.Name())
	}
}

func TestReadRootLines(t *testing.T) {
	test := map[string]*struct {
		expectRoot *FileNode
	}{
		"RootA": {
			expectRoot: GetRootA(),
		},
	}

	for n, c := range test {
		t.Run(n, func(t *testing.T) {
			teardown := createRootFile(c.expectRoot)
			defer teardown()
			mapp := ReadRootLines("test.txt")
			_, ok := mapp[c.expectRoot.Id]
			assert.True(t, ok)
		})
	}
}

func TestRootDeserialize(t *testing.T) {
	rootA := GetRootA()
	teardown := createRootFile(rootA)
	defer teardown()
	nodeMap := ReadRootLines("test.txt")
	rootB := RootDeserialize(nodeMap)
	assert.True(t, rootA.IsDeepEqualTo(rootB))
}
