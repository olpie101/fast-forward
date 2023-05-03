package nats

type ProjectionConfig struct {
	Name       string            `json:"name,omitempty" mapstructure:"name,omitempty"`
	Enabled    bool              `json:"enabled,omitempty" mapstructure:"enabled,omitempty"`
	Durable    bool              `json:"durable,omitempty" mapstructure:"durable,omitempty"`
	QueueGroup string            `json:"queue_group,omitempty" mapstructure:"queue_group,omitempty"`
	KVName     string            `json:"kv_name,omitempty" mapstructure:"kv_name,omitempty"`
	KVNames    map[string]string `json:"kv_names,omitempty" mapstructure:"kv_names,omitempty"`
	NoKV       bool              `json:"no_kv,omitempty" mapstructure:"no_kv,omitempty"`
}
