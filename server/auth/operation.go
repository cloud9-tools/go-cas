package auth

//go:generate stringer -type=Operation

type Operation uint8

const (
	Any Operation = iota
	StatFS
	Walk
	Get
	Put
	Remove
)
