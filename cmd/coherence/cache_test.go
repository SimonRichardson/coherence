package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func TestCluster(t *testing.T) {
	nodes := generateCluster(t, 3)
	defer func() {
		nodes.Close()
	}()

	time.Sleep(time.Second * 6)

	fmt.Println("run")
}

type Nodes struct {
	mutex sync.Mutex
	nodes []Node
}

func (n *Nodes) Append(node Node) {
	n.mutex.Lock()
	n.nodes = append(n.nodes, node)
	n.mutex.Unlock()
}

func (n *Nodes) Close() {
	n.mutex.Lock()
	for _, v := range n.nodes {
		v.Close()
	}
	n.mutex.Unlock()
}

type Node struct {
	API, ClusterAPI string
	cancel          context.CancelFunc
}

func (n Node) Close() {
	n.cancel()
}

func generateCluster(t *testing.T, amount int) *Nodes {
	nodes := new(Nodes)
	for i := 0; i < amount; i++ {
		go func(i int) {
			port := 9090 + i
			args := []string{
				"cache", "-debug",
				fmt.Sprintf("-api=tcp://0.0.0.0:%d", port),
			}

			if i > 0 {
				args = append(args,
					fmt.Sprintf("-cluster=tcp://0.0.0.0:%d", 8080+i),
					fmt.Sprintf("-peer=0.0.0.0:%d", defaultClusterPort),
				)
			}

			cmd := exec.Command("../../dist/coherence", args...)
			cmd.Stderr = os.Stderr
			cmd.Stdout = os.Stdout

			nodes.Append(Node{
				API:        fmt.Sprintf("http://0.0.0.0:%d", port),
				ClusterAPI: fmt.Sprintf("http://0.0.0.0:%d", 8080+i),
				cancel: func() {
					if err := cmd.Process.Kill(); err != nil {
						log.Fatal(err)
					}
				},
			})

			if err := cmd.Run(); err != nil {
				t.Fatal(err)
			}
		}(i)
	}

	return nodes
}
