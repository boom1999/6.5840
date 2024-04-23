package mr

type MapSet struct {
	mapbool map[interface{}]bool
	count   int
}

func NewMapSet() *MapSet {
	return &MapSet{mapbool: make(map[interface{}]bool), count: 0}
}

func (m *MapSet) Add(key interface{}) {
	m.mapbool[key] = true
	m.count++
}

func (m *MapSet) Remove(key interface{}) {
	delete(m.mapbool, key)
	m.count--
}

func (m *MapSet) Contains(key interface{}) bool {
	_, ok := m.mapbool[key]
	return ok
}

func (m *MapSet) Size() int {
	return m.count
}
