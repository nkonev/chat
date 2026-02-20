package cqrs

type CqrsEvent interface {
	GetPartitionKey() string
	Name() string
	GetEventKind() EventKind
}
