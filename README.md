# badgertest

```bash
$ bg -gen -dir /tmp/badgergo -count 1000000 -keysize 10 -valsize 100
$ bg -read -dir /tmp/badgergo -key hello
$ bg -write -dir /tmp/badgergo -key hello -value world
```