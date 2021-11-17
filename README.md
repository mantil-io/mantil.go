# mantil.go

[![License: MIT][License-Image]][License-Url] [![GoDoc][GoDoc-Image]][GoDoc-Url] [![Go Report Card][GoReport-Image]][GoReport-Url]

[License-Url]: https://opensource.org/licenses/MIT
[License-Image]: https://img.shields.io/badge/License-MIT-blue.svg
[GoDoc-Url]: https://pkg.go.dev/github.com/mantil-io/mantil.go
[GoDoc-Image]: https://img.shields.io/badge/GoDoc-reference-007d9c
[GoReport-Image]: https://goreportcard.com/badge/github.com/mantil-io/mantil.go
[GoReport-Url]: https://goreportcard.com/report/github.com/mantil-io/mantil.go


mantil.go integrates Lambda function with API's in a Mantil project.

It is similar to the default AWS Go [Lambda function
handler](https://docs.aws.amazon.com/lambda/latest/dg/golang-handler.html). The
main difference is that mantil.go handler mantil.LmabdaHandler accepts struct
instance and exposes each exported method of that struct. Where the default
implementation has a single function as an entrypoint.

Package is intented for usage inside Mantil project. 

Package also provides simple key value store interface backed by a DynamoDB table.
It manages that table as part of the Mantil project. It is created on demand and
destroyed with the project.
