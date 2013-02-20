# github.com/nuance/kafka

## Example

A simple, go-like Go Kafka consumer. Use it like follows:
    package main

    import (
        "fmt"
        "github.com/Codefor/kafka"
    )

    func main() {
        conn, _ := kafka.OpenConsumer("10.4.32.221:9092", "ddclick", 0,
            kafka.OFF_NEWEST, kafka.DefaultConsumerOptions())

        data,_ := conn.Fetch()
        for _,str := range data{
            fmt.Println(str)
        }
    }

The kafka module exposes an io.ReadCloser-compatible interface:

    type Consumer interface {
    	Seek(offset int64)
    	Read(buf []byte) (int, error)
    	GetOffset() int64
    	Close() error

    	Offsets(base int64, num int32) ([]int64, error)
    }

Read will always read exactly one message or fail. Message fetches, however, are batched behind the scene, so calls to Read do not correspond 1:1 with network requests.

## Warning

This API is subject to change. I'd like to eventually add support for publishers, and probably move the Offsets call off of the reader interface.
