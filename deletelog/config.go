package deletelog

type configGetter interface {
	GetDeletion() Config
}

type Config struct {
	Enable bool
}
