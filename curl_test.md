# Test the application using Curl

```shell
$ curl -X POST http://localhost:8080/s3/upload -H 'Content-Type: multipart/form-data' -F 'file=@"./README.md"' -F 'payload="{}";type=application/json' -w '\n'
```
