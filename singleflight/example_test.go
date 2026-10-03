package singleflight_test

import (
	"fmt"

	"github.com/snowmerak/toybox/singleflight"
)

func ExampleGroup() {
	var g singleflight.Group
	gate := make(chan struct{})
	first := g.DoChan("user:42", func() (any, error) {
		<-gate
		return "Alice", nil
	})
	second := g.DoChan("user:42", func() (any, error) {
		return "not executed", nil
	})
	close(gate)
	for _, ch := range []<-chan singleflight.Result{first, second} {
		result := <-ch
		fmt.Println(result.Val, result.Shared)
	}
	// Output:
	// Alice true
	// Alice true
}

func ExampleNewWithOptions() {
	group := singleflight.NewWithOptions(singleflight.DefaultShards, singleflight.Options{
		WaitMode: singleflight.ChannelMode,
	})
	value, err, shared := group.Do("user:42", func() (any, error) {
		return "Alice", nil
	})
	fmt.Println(value, err, shared)
	// Output: Alice <nil> false
}
