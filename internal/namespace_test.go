package internal

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"tinydfs-base/common"
	"tinydfs-base/util"
)

type NodeTestCase struct {
	initRootFunc        func(path string)
	path                string
	isRead              bool
	expectIsFileExist   bool
	expectStackLength   int
	expectCanReadLockOn bool
	expectFileName      string
}

// initRoot 根据path构造目录树
func initRoot(path string) {
	pp := strings.Split(path, pathSplitString)
	n := len(pp)
	nextNode := &FileNode{
		Id:         util.GenerateUUIDString(),
		FileName:   pp[n-1],
		ChildNodes: map[string]*FileNode{},
		IsFile:     true,
	}
	for i := n - 2; i >= 1; i-- {
		curNode := &FileNode{
			Id:         util.GenerateUUIDString(),
			FileName:   pp[i],
			ChildNodes: map[string]*FileNode{},
			IsFile:     false,
		}
		curNode.ChildNodes[nextNode.FileName] = nextNode
		nextNode.ParentNode = curNode
		nextNode = curNode
	}
	root.ChildNodes[nextNode.FileName] = nextNode
	nextNode.ParentNode = root
}

func TestGetAndLockByPath(t *testing.T) {
	test := map[string]*NodeTestCase{
		"PathNotExist": {
			initRootFunc:        nil,
			path:                "/usr/local/abc.txt",
			isRead:              true,
			expectIsFileExist:   false,
			expectStackLength:   0,
			expectCanReadLockOn: true,
		},
		"Read": {
			initRootFunc:        initRoot,
			path:                "/usr/local/abc.txt",
			isRead:              true,
			expectIsFileExist:   true,
			expectStackLength:   4,
			expectCanReadLockOn: true,
		},
		"Write": {
			initRootFunc:        initRoot,
			path:                "/usr/local/abc.txt",
			isRead:              false,
			expectIsFileExist:   true,
			expectStackLength:   4,
			expectCanReadLockOn: false,
		},
		"WriteDirectory": {
			initRootFunc:        initRoot,
			path:                "/usr/local/",
			isRead:              false,
			expectIsFileExist:   true,
			expectStackLength:   3,
			expectCanReadLockOn: false,
		},
		"WriteDirectory2": {
			initRootFunc:        initRoot,
			path:                "/usr/local",
			isRead:              false,
			expectIsFileExist:   true,
			expectStackLength:   3,
			expectCanReadLockOn: false,
		},
	}
	for name, c := range test {
		t.Run(name, func(t *testing.T) {
			defer func() {
				root.ChildNodes = map[string]*FileNode{}
			}()
			if c.initRootFunc != nil {
				c.initRootFunc(c.path)
			}
			_, exist := getFileNode(c.path)
			assert.Equal(t, c.expectIsFileExist, exist)
		})
	}
}

func TestCheckAndGetFileNode(t *testing.T) {
	test := map[string]*NodeTestCase{
		"FileExist": {
			initRootFunc:   initRoot,
			path:           "/usr/local/abc.txt",
			expectFileName: "abc.txt",
		},
		"FileNotExist": {
			initRootFunc:   nil,
			path:           "/usr/local/abc.txt",
			expectFileName: "abc.txt",
		},
	}
	for name, c := range test {
		t.Run(name, func(t *testing.T) {
			defer func() {
				root.ChildNodes = map[string]*FileNode{}
			}()
			if c.initRootFunc != nil {
				c.initRootFunc(c.path)
			}
			node, err := CheckAndGetFileNode(c.path)
			if c.initRootFunc == nil {
				assert.Nil(t, node)
				assert.Error(t, err)
			} else {
				assert.Equal(t, c.expectFileName, node.FileName)
			}
		})
	}
}

func TestInitChunks(t *testing.T) {
	test := map[string]*struct {
		size                 int64
		id                   string
		expectFirstChunkName string
		expectLastChunkName  string
	}{
		"a": {
			size:                 common.ChunkSize,
			id:                   "a",
			expectFirstChunkName: "a0",
			expectLastChunkName:  "a0",
		},
		"b": {
			size:                 common.ChunkSize - 1,
			id:                   "b",
			expectFirstChunkName: "b0",
			expectLastChunkName:  "b0",
		},
		"c": {
			size:                 common.ChunkSize + 1,
			id:                   "c",
			expectFirstChunkName: "c0",
			expectLastChunkName:  "c1",
		},
	}
	for name, c := range test {
		t.Run(name, func(t *testing.T) {
			res := initChunks(c.size, c.id)
			assert.Equal(t, c.expectFirstChunkName, res[0])
			assert.Equal(t, c.expectLastChunkName, res[len(res)-1])
		})
	}
}

func TestRemoveFileNode(t *testing.T) {
	test := map[string]*struct {
		initRoot    func(path string)
		path        string
		expectIsDel bool
	}{
		"FileNotExist": {
			initRoot: nil,
			path:     "/a/b/c.txt",
		},
		"FileExist": {
			initRoot:    initRoot,
			path:        "/a/b/c.txt",
			expectIsDel: true,
		},
	}
	for name, c := range test {
		t.Run(name, func(t *testing.T) {
			defer func() {
				root.ChildNodes = map[string]*FileNode{}
			}()
			if c.initRoot != nil {
				c.initRoot(c.path)
			}
			node, err := RemoveFileNode(c.path)
			if c.initRoot == nil {
				assert.Nil(t, node)
				assert.Error(t, err)
			} else {
				assert.Equal(t, c.expectIsDel, node.IsDel)
			}
		})
	}
}

func TestListFileNode(t *testing.T) {
	test := map[string]*struct {
		initRoot        func(path string)
		directory       string
		path            string
		expectFileNames []string
	}{
		"FileNotExist": {
			initRoot: nil,
			path:     "/a/b/",
		},
		"FileExist": {
			initRoot:        initRoot,
			directory:       "/a/b/c.txt",
			path:            "/a/b/",
			expectFileNames: []string{"c.txt"},
		},
	}
	for name, c := range test {
		t.Run(name, func(t *testing.T) {
			defer func() {
				root.ChildNodes = map[string]*FileNode{}
			}()
			if c.initRoot != nil {
				c.initRoot(c.directory)
			}
			nodes, err := ListFileNode(c.path)
			if c.initRoot == nil {
				assert.Nil(t, nodes)
				assert.Error(t, err)
			} else {
				for i, s := range nodes {
					assert.Equal(t, c.expectFileNames[i], s.FileName)
				}
			}
		})
	}
}

func TestRenameFileNode(t *testing.T) {
	test := map[string]*struct {
		initRoot       func(path string)
		directory      string
		path           string
		newName        string
		expectFileName string
	}{
		"FileNotExist": {
			initRoot: nil,
			path:     "/a/b/",
		},
		"FileExist": {
			initRoot:       initRoot,
			directory:      "/a/b/c.txt",
			path:           "/a/b/c.txt",
			newName:        "newName.txt",
			expectFileName: "newName.txt",
		},
	}
	for name, c := range test {
		t.Run(name, func(t *testing.T) {
			defer func() {
				root.ChildNodes = map[string]*FileNode{}
			}()
			if c.initRoot != nil {
				c.initRoot(c.directory)
			}
			node, err := RenameFileNode(c.path, c.newName)
			if c.initRoot == nil {
				assert.Nil(t, node)
				assert.Error(t, err)
			} else {
				assert.Equal(t, c.expectFileName, node.FileName)
			}
		})
	}
}

func TestFileNodeString(t *testing.T) {
	test := map[string]*struct {
		node         *FileNode
		expectString string
	}{
		"RootA": {
			node:         GetRootA(),
			expectString: "",
		},
		"Root": {
			node:         root,
			expectString: "",
		},
	}
	for name, c := range test {
		t.Run(name, func(t *testing.T) {
			if name == "RootA" {
				fmt.Println(c.node.String())
				for _, n := range c.node.ChildNodes {
					fmt.Println(n.String())
				}
			}
		})
	}
}

// getRootA returns /b.txt /c directory
func GetRootA() *FileNode {
	a := &FileNode{
		Id:         util.GenerateUUIDString(),
		FileName:   "",
		ParentNode: nil,
		ChildNodes: map[string]*FileNode{},
		Chunks:     nil,
		Size:       0,
		IsFile:     false,
	}
	b := &FileNode{
		Id:         util.GenerateUUIDString(),
		FileName:   "b.txt",
		ParentNode: a,
		ChildNodes: nil,
		Chunks:     []string{"chunk1", "chunk2"},
		Size:       2042,
		IsFile:     true,
	}
	a.ChildNodes[b.FileName] = b
	c := &FileNode{
		Id:         util.GenerateUUIDString(),
		FileName:   "c",
		ParentNode: a,
		ChildNodes: map[string]*FileNode{},
		Chunks:     nil,
		Size:       0,
		IsFile:     false,
	}
	a.ChildNodes[c.FileName] = c
	return a
}
