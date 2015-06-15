package internal

//go:generate stringer -type=Comparison

type Comparison int

const (
	LessThan    Comparison = -1
	EqualTo     Comparison = 0
	GreaterThan Comparison = 1
)
