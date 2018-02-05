package main

import (
	"fmt"
	"strings"
	"testing"
	"testing/quick"

	"github.com/trussle/harness/generators"
)

func TestPeersSlice(t *testing.T) {
	fn := func(a []generators.ASCII) bool {
		var ss peersSlice
		for _, v := range a {
			if err := ss.Set(v.String()); err != nil {
				t.Fatal(err)
			}
		}
		if expected, actual := join(a), strings.Join(ss, " "); expected != actual {
			t.Errorf("expected: %q, actual: %q", expected, actual)
		}
		return true
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func join(a []generators.ASCII) string {
	res := make([]string, len(a))
	for k, v := range a {
		res[k] = fmt.Sprintf("%s:%d", strings.ToLower(v.String()), defaultClusterPort)
	}
	return strings.Join(res, " ")
}

func TestParseAddr(t *testing.T) {
	for _, testcase := range []struct {
		addr        string
		defaultPort int
		network     string
		address     string
	}{
		{"foo", 123, "tcp", "foo:123"},
		{"foo:80", 123, "tcp", "foo:80"},
		{"udp://foo", 123, "udp", "foo:123"},
		{"udp://foo:8080", 123, "udp", "foo:8080"},
		{"tcp+dnssrv://testing:7650", 7650, "tcp+dnssrv", "testing:7650"},
	} {
		network, address, err := parseAddr(testcase.addr, testcase.defaultPort)
		if err != nil {
			t.Errorf("(%q, %d): %v", testcase.addr, testcase.defaultPort, err)
			continue
		}
		var (
			matchNetwork = network == testcase.network
			matchAddress = address == testcase.address
		)
		if !matchNetwork || !matchAddress {
			t.Errorf("(%q, %d): want [%s %s], have [%s %s]",
				testcase.addr, testcase.defaultPort,
				testcase.network, testcase.address,
				network, address,
			)
			continue
		}
	}
}

func TestHasNonlocal(t *testing.T) {
	makeslice := func(a ...string) []string {
		ss := peersSlice{}
		for _, s := range a {
			if err := ss.Set(s); err != nil {
				t.Fatal(err)
			}
		}
		return ss.Slice()
	}
	for _, testcase := range []struct {
		name  string
		input peersSlice
		want  bool
	}{
		{
			"empty",
			makeslice(),
			false,
		},
		{
			"127",
			makeslice("127.0.0.9"),
			false,
		},
		{
			"127 with port",
			makeslice("127.0.0.1:1234"),
			false,
		},
		{
			"nonlocal IP",
			makeslice("1.2.3.4"),
			true,
		},
		{
			"nonlocal IP with port",
			makeslice("1.2.3.4:5678"),
			true,
		},
		{
			"nonlocal host",
			makeslice("foo.corp"),
			true,
		},
		{
			"nonlocal host with port",
			makeslice("foo.corp:7659"),
			true,
		},
		{
			"localhost",
			makeslice("localhost"),
			false,
		},
		{
			"localhost with port",
			makeslice("localhost:1234"),
			false,
		},
		{
			"multiple IP",
			makeslice("127.0.0.1", "1.2.3.4"),
			true,
		},
		{
			"multiple hostname",
			makeslice("localhost", "otherhost"),
			true,
		},
		{
			"multiple local",
			makeslice("localhost", "127.0.0.1", "127.128.129.130:4321", "localhost:10001", "localhost:10002"),
			false,
		},
		{
			"multiple mixed",
			makeslice("localhost", "127.0.0.1", "1.2.3.4", "129.128.129.130:4321", "localhost:10001", "localhost:10002"),
			true,
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			if want, have := testcase.want, hasNonlocal(testcase.input); want != have {
				t.Errorf("expected: %v, actual %v, %s", testcase.want, testcase.input, testcase.name)
			}
		})
	}
}

func TestIsUnroutable(t *testing.T) {
	for _, testcase := range []struct {
		input string
		want  bool
	}{
		{"0.0.0.0", true},
		{"127.0.0.1", true},
		{"127.128.129.130", true},
		{"localhost", true},
		{"foo", false},
		{"::", true},
	} {
		t.Run(testcase.input, func(t *testing.T) {
			if want, have := testcase.want, isUnroutable(testcase.input); want != have {
				t.Errorf("want %v, have %v", want, have)
			}
		})
	}
}
