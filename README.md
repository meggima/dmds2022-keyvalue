# dmds2022-keyvalue

Lukas Bosshart

Markus Eggimann

## Sources and Inspirations

- https://www.javatpoint.com/b-plus-tree
- https://pkg.go.dev/github.com/google/btree

## Coverage

Install coverage tool: `go get golang.org/x/tools/cmd/cover`

```
go test -v -coverprofile coverage.out &&
go tool cover -html coverage.out -o coverage.html
open coverage.html
```