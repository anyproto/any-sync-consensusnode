package db

type configGetter interface {
	GetDB() Config
}

type Config struct {
	Connect       string `yaml:"connect"`
	Database      string `yaml:"database"`
	LogCollection string `yaml:"logCollection"`
}
