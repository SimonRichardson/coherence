package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/SimonRichardson/flagset"
	"github.com/pkg/errors"
)

var version = "dev"

const (
	defaultAPIPort     = 8080
	defaultClusterPort = 8079
	defaultAddr        = "0.0.0.0:0"
)

var (
	defaultAPIAddr     = fmt.Sprintf("tcp://0.0.0.0:%d", defaultAPIPort)
	defaultClusterAddr = fmt.Sprintf("tcp://0.0.0.0:%d", defaultClusterPort)
)

type command func([]string) error

func (c command) Run(args []string) {
	if err := c(args); c != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func main() {
	args := os.Args

	if len(args) < 2 {
		if mode, ok := syscall.Getenv("MODE"); ok {
			args = append(args, mode)
		} else {
			usage()
			os.Exit(1)
		}
	}

	var cmd command
	switch strings.ToLower(args[1]) {
	case "cache":
		cmd = runCache
	default:
		usage()
		os.Exit(1)
	}

	cmd.Run(args[2:])
}

func usage() {
	fmt.Fprintf(os.Stderr, "USAGE\n")
	fmt.Fprintf(os.Stderr, "  %s <mode> [flags]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "MODES\n")
	fmt.Fprintf(os.Stderr, "  cache       Cache service\n")
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "VERSION\n")
	fmt.Fprintf(os.Stderr, "  %s (%s)\n", version, runtime.Version())
	fmt.Fprintf(os.Stderr, "\n")
}

func usageFor(fs *flagset.FlagSet, name string) func() {
	return func() {
		fmt.Fprintf(os.Stderr, "USAGE\n")
		fmt.Fprintf(os.Stderr, "  %s\n", name)
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "FLAGS\n")

		writer := tabwriter.NewWriter(os.Stderr, 0, 2, 2, ' ', 0)
		fs.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(writer, "\t-%s %s\t%s\n", f.Name, f.DefValue, f.Usage)
		})
		writer.Flush()

		fmt.Fprintf(os.Stderr, "\n")
	}
}

func errorFor(fs *flagset.FlagSet, name string, err error) error {
	defer usageFor(fs, name)()

	if err != nil {
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "ERROR\n")
		fmt.Fprintf(os.Stderr, "  %s\n", err.Error())
		fmt.Fprintf(os.Stderr, "\n---------------------------------------------\n\n")

		// Suppress the original error.
		return errors.Errorf("")
	}

	return err
}
