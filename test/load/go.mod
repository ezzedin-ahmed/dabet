module dabet/test/load

go 1.26

require (
	dabet/pkg v0.0.0
	github.com/twmb/franz-go v1.21.6
	github.com/twmb/franz-go/pkg/kadm v1.18.0
)

require (
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.54.0 // indirect
)

replace dabet/pkg => ../../pkg
