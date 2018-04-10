package concepts

// Options for the Neo4j ODM
type Options struct {
	PrimaryKeyName          string
	SkipRelations           bool
	IfNotModified           bool
	NotModifiedAttributeKey string
}

// Parse will modify our full Options according to the given Option setters
func (o *Options) Parse(setters []Option) {
	for _, setter := range setters {
		setter(o)
	}
}

// NewDefaultOptions provides our default Options
func NewDefaultOptions() *Options {
	return &Options{
		PrimaryKeyName:          "uuid",
		SkipRelations:           false,
		IfNotModified:           false,
		NotModifiedAttributeKey: "aggregateHash",
	}
}

// Option fn we use to manage our Options
type Option func(*Options)

// PrimaryKeyName to set Options.PrimaryKeyName
func PrimaryKeyName(name string) Option {
	return func(args *Options) {
		args.PrimaryKeyName = name
	}
}

// SkipRelations to set Options.SkipRelations to true
func SkipRelations() Option {
	return func(args *Options) {
		args.SkipRelations = true
	}
}

// IfNotModified to set Options.IfNotModified to true
func IfNotModified() Option {
	return func(args *Options) {
		args.IfNotModified = true
	}
}

// NotModifiedAttributeKey to set Options.NotModifiedAttributeKey
func NotModifiedAttributeKey(name string) Option {
	return func(args *Options) {
		args.NotModifiedAttributeKey = name
	}
}
