package main

import (
	"context"
	"fmt"
	"time"

	"github.com/guoyyit/dynamic-batching-serving-go"
)

func main() {
	engine := serving.NewStubEngine()
	cfg := serving.Config{
		Version: 1,
		Global:  serving.GlobalConfig{TickInterval: 2 * time.Millisecond, NumWorkers: 2},
		Models: map[string]serving.ModelConfig{
			"m1": {Name: "m1", MaxBatchSize: 8, BatchWindow: 5 * time.Millisecond, QueueCapacity: 64, RequestTimeout: time.Second, RetryStrategy: serving.RetryNever, OverflowPolicy: serving.OverflowReject},
		},
	}
	s, err := serving.New(cfg, engine)
	if err != nil {
		panic(err)
	}
	defer s.Close()

	out, err := s.Submit(context.Background(), serving.Request{Model: "m1", Payload: "hello"})
	fmt.Println("result:", out, "err:", err)
}
