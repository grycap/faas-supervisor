package main

import (
	"fmt"
	"os"

	"github.com/faassupervisor/go/internal/supervisor"
	"github.com/faassupervisor/go/internal/utils"
)

func main() {
	if utils.IsLambdaImageEnvironment() {
		fmt.Fprintln(os.Stderr, "Lambda Image mode requires the awslambdaric runtime; the Go binary only supports binary mode.")
		os.Exit(1)
	}

	ret := run(stdinEvent(), nil)
	if ret != nil {
		switch v := ret.(type) {
		case string:
			fmt.Println(v)
		case []byte:
			fmt.Println(string(v))
		default:
			fmt.Println(v)
		}
	}
}

func stdinEvent() string {
	return utils.GetStdin()
}

func run(event interface{}, context interface{}) interface{} {
	g := supervisor.NewGenericSupervisor(event, context)
	return g.Run()
}