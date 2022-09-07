package internal

const (
	Operation_Add    = "Add"
	Operation_Remove = "Remove"
	Operation_Move   = "Move"
	Operation_Rename = "Rename"
)

type Operation struct {
	Type   string // Specify the Operation Type
	Src    string // Client `src` Parameter
	Des    string // Client `des` Parameter
	IsFile bool   // Available if Type if Add
}

func OperationAdd(des string, isFile bool) *Operation {
	return &Operation{
		Type:   Operation_Add,
		Des:    des,
		IsFile: isFile,
	}
}

func OperationRemove(des string) *Operation {
	return &Operation{
		Type: Operation_Remove,
		Des:  des,
	}
}

func OperationRename(src, des string) *Operation {
	return &Operation{
		Type: Operation_Rename,
		Src:  src,
		Des:  des,
	}
}

func OperationMove(src, des string) *Operation {
	return &Operation{
		Type: Operation_Move,
		Src:  src,
		Des:  des,
	}
}
