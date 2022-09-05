package internal

const (
	Operation_Add    = "Add"
	Operation_Remove = "Remove"
	Operation_Move   = "Move"
	Operation_Rename = "Rename"
)

type Operation struct {
	Type string
}
