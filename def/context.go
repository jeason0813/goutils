package def

type Context interface{
	Logger() Logger
	Set(key string, value interface{})
	Get(key string) (interface{}, bool)
}