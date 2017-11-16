package cluster

import (
	"testing"

	sockaddr "github.com/hashicorp/go-sockaddr"
)

func TestCalculateAdvertiseAddr(t *testing.T) {
	privateIP, err := sockaddr.GetPrivateIP()
	if err != nil {
		t.Fatal(err)
	}

	for _, testcase := range []struct {
		name          string
		bindAddr      string
		advertiseAddr string
		want          string
	}{
		{"Public bind no advertise",
			"1.2.3.4", "", "1.2.3.4",
		},
		{"Private bind no advertise",
			"10.1.2.3", "", "10.1.2.3",
		},
		{"Zeroes bind public advertise",
			"0.0.0.0", "2.3.4.5", "2.3.4.5",
		},
		{"Zeroes bind private advertise",
			"0.0.0.0", "172.16.1.9", "172.16.1.9",
		},
		{"Zeroes bind private ip",
			"0.0.0.0", "", privateIP,
		},
		{"Public bind private advertise",
			"188.177.166.155", "10.11.12.13", "10.11.12.13",
		},
		{"IPv6 bind no advertise",
			"::", "", "::",
		},
		{"IPv6 bind private advertise",
			"::", "172.16.1.1", "172.16.1.1",
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			ip, err := CalculateAdvertiseAddress(testcase.bindAddr, testcase.advertiseAddr)
			if err != nil {
				t.Fatal(err)
			}
			if want, have := testcase.want, ip.String(); want != have {
				t.Fatalf("want '%s', have '%s'", want, have)
			}
		})
	}
}
