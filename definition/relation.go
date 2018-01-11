package definition

type ID interface {
	ID() string
}

type Member interface {
	ID
	Delete(component Component) error
}

type Component interface {
	ID
	Group(category string) bool
	Remove(member Member) error
	Join(member Member) error
}
