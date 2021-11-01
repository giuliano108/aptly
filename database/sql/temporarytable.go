package sql

import "sync"

type temporaryTableIDGenerator struct {
	sync.Mutex
	id uint
}

func (i *temporaryTableIDGenerator) Get() uint {
	i.Lock()
	defer i.Unlock()
	i.id++
	return i.id
}

var temporaryTableID temporaryTableIDGenerator = temporaryTableIDGenerator{id: 0}
