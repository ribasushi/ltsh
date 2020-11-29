package nullcache

type NullCache bool

const Instance = NullCache(true)

func (*NullCache) Get(interface{}) (interface{}, bool)           { return nil, false }
func (*NullCache) Peek(interface{}) (value interface{}, ok bool) { return nil, false }
func (*NullCache) Contains(interface{}) bool                     { return false }
func (*NullCache) Len() int                                      { return 0 }
func (*NullCache) Keys() []interface{}                           { return nil }
func (*NullCache) Add(interface{}, interface{})                  {}
func (*NullCache) Remove(interface{})                            {}
func (*NullCache) Purge()                                        {}
