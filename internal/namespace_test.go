package internal

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"sync"
	"testing"
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
	//TODO 最后一个是否为文件
	n := len(pp)
	nextNode := &FileNode{
		Id:             util.GenerateUUIDString(),
		FileName:       pp[n-1],
		ChildNodes:     map[string]*FileNode{},
		IsFile:         true,
		UpdateNodeLock: &sync.RWMutex{},
	}
	for i := n - 2; i >= 1; i-- {
		curNode := &FileNode{
			Id:             util.GenerateUUIDString(),
			FileName:       pp[i],
			ChildNodes:     map[string]*FileNode{},
			IsFile:         false,
			UpdateNodeLock: &sync.RWMutex{},
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
			_, s, exist := getAndLockByPath(c.path, c.isRead)
			assert.Equal(t, c.expectIsFileExist, exist)
			assert.Equal(t, c.expectStackLength, s.Len())
			// 判断最后一个是读锁还是写锁
			if s.Len() != 0 {
				node, ok := s.Back().Value.(*FileNode)
				assert.True(t, ok)
				assert.Equal(t, c.expectCanReadLockOn, node.updateNodeLock.TryRLock())
				unlockAllMutex(s, c.isRead)
			}
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
			size:                 1024,
			id:                   "a",
			expectFirstChunkName: "a0",
			expectLastChunkName:  "a0",
		},
		"b": {
			size:                 1023,
			id:                   "b",
			expectFirstChunkName: "b0",
			expectLastChunkName:  "b0",
		},
		"c": {
			size:                 1025,
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
				root.childNodes = map[string]*FileNode{}
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
