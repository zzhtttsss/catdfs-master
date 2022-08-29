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

func initRoot(path string) {
	pp := strings.Split(path, pathSplitString)
	//TODO 最后一个是否为文件
	n := len(pp)
	nextNode := &FileNode{
		FileName:       pp[n-1],
		childNodes:     map[string]*FileNode{},
		IsFile:         true,
		updateNodeLock: &sync.RWMutex{},
	}
	for i := n - 2; i >= 1; i-- {
		curNode := &FileNode{
			FileName:       pp[i],
			childNodes:     map[string]*FileNode{},
			IsFile:         false,
			updateNodeLock: &sync.RWMutex{},
		}
		curNode.childNodes[nextNode.FileName] = nextNode
		nextNode = curNode
	}
	root.childNodes[nextNode.FileName] = nextNode
}

func TestGetAndLockByPath(t *testing.T) {
	test := map[string]*NodeTestCase{
		"PathNotExit": {
			initRootFunc:        nil,
			path:                "/usr/local/abc.txt",
			isRead:              true,
			expectIsFileExist:   false,
			expectStackLength:   1,
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
				root.childNodes = map[string]*FileNode{}
			}()
			if c.initRootFunc != nil {
				c.initRootFunc(c.path)
			}
			_, s, exist := getAndLockByPath(c.path, c.isRead)
			assert.Equal(t, c.expectIsFileExist, exist)
			assert.Equal(t, c.expectStackLength, s.Len())
			// 判断最后一个是读锁还是写锁
			node, ok := s.Back().Value.(*FileNode)
			assert.True(t, ok)
			assert.Equal(t, c.expectCanReadLockOn, node.updateNodeLock.TryRLock())
			UnlockAllMutex(s, c.isRead)
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
				root.childNodes = map[string]*FileNode{}
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
