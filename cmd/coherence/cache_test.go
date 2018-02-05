package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/SimonRichardson/coherence/pkg/api"
	"github.com/pkg/errors"
)

func TestCluster(t *testing.T) {
	nodes := generateCluster(t, 8080, 3)
	defer func() {
		nodes.Close()
	}()

	serverURL := nodes.Get(0).API
	_, code, err := selectValue(serverURL, "key1", "field1")
	if code != 404 {
		dumpStdErr(nodes)
		t.Fatalf("expected: 404, actual: %d \nerr: %s\n", code, err.Error())
	}

	code, err = insertValue(serverURL, "key1", "field1", "value1", 1)
	if code != 200 {
		dumpStdErr(nodes)
		t.Fatalf("expected: 200, actual: %d \nerr: %s\n", code, err.Error())
	}

	value, code, err := selectValue(serverURL, "key1", "field1")
	if code != 200 {
		dumpStdErr(nodes)
		t.Fatalf("expected: 200, actual: %d \nerr: %s\n", code, err.Error())
	}

	fmt.Println(value)
}

type Nodes struct {
	mutex sync.Mutex
	nodes []Node
}

func NewNodes(amount int) *Nodes {
	return &Nodes{
		nodes: make([]Node, amount),
	}
}

func (n *Nodes) Insert(index int, node Node) {
	n.mutex.Lock()
	n.nodes[index] = node
	n.mutex.Unlock()
}

func (n *Nodes) Get(index int) Node {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	return n.nodes[index]
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
	stdOut, stdErr  *bytes.Buffer
	cancel          context.CancelFunc
}

func (n Node) Close() {
	n.cancel()
}

func (n Node) StdOut() string {
	return n.stdOut.String()
}

func (n Node) StdErr() string {
	return n.stdErr.String()
}

func dumpStdErr(n *Nodes) {
	for k, v := range n.nodes {
		fmt.Printf("OUT: %d %s\n", k, v.StdOut())
		fmt.Printf("ERR: %d %s\n------------\n", k, v.StdErr())
	}
}

func generateCluster(t *testing.T, port, amount int) *Nodes {
	nodes := NewNodes(amount)
	for i := 0; i < amount; i++ {
		go func(i int) {
			args := []string{
				"cache", "-debug",
				fmt.Sprintf("-api=tcp://0.0.0.0:%d", port+i),
			}

			clusterPort := (port + 100) + i

			if i > 0 {
				args = append(args,
					fmt.Sprintf("-cluster=tcp://0.0.0.0:%d", clusterPort),
					fmt.Sprintf("-peer=0.0.0.0:%d", defaultClusterPort),
				)
			}

			var (
				stdOut = new(bytes.Buffer)
				stdErr = new(bytes.Buffer)
			)

			cmd := exec.Command("../../dist/coherence", args...)
			cmd.Stderr = stdOut
			cmd.Stdout = stdErr

			nodes.Insert(i, Node{
				API:        fmt.Sprintf("http://0.0.0.0:%d", port),
				ClusterAPI: fmt.Sprintf("http://0.0.0.0:%d", clusterPort),
				stdOut:     stdOut,
				stdErr:     stdErr,
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

	time.Sleep(time.Second * 2)

	return nodes
}

func selectValue(url, key, field string) (string, int, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/cache/select?key=%s&field=%s", url, key, field), nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	if resp.StatusCode != 200 {
		var s struct {
			Description string `json:"description"`
			Code        int    `json:"code"`
		}
		if err := json.Unmarshal(bytes, &s); err != nil {
			log.Fatal(err)
		}
		return "", resp.StatusCode, errors.Errorf("description: %s, code: %d", s.Description, s.Code)
	}

	var s struct {
		Records struct {
			Field string `json:"field"`
			Value string `json:"value"`
			Score int    `json:"score"`
		} `json:"records"`
	}
	if err := json.Unmarshal(bytes, &s); err != nil {
		log.Fatal(err)
	}

	return s.Records.Value, resp.StatusCode, nil
}

func insertValue(url, key, field, value string, score int64) (int, error) {
	b, err := json.Marshal(api.MembersInput{
		Members: []api.FieldValueScore{
			api.FieldValueScore{
				Field: api.Field(field),
				Value: []byte(value),
				Score: score,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/cache/insert?key=%s", url, key), bytes.NewReader(b))
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	if resp.StatusCode != 200 {
		var s struct {
			Description string `json:"description"`
			Code        int    `json:"code"`
		}
		if err := json.Unmarshal(bytes, &s); err != nil {
			log.Fatal(err)
		}
		return resp.StatusCode, errors.Errorf("description: %s, code: %d", s.Description, s.Code)
	}

	return resp.StatusCode, nil
}
