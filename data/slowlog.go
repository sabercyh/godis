package data

type SlowLogEntry struct {
	Robj     []*Gobj
	Argc     int
	ID       int64
	Duration int64
	Time     int64
}
