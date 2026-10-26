package harness

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// Container names of the dev stack (Compose project "runsafe").
const (
	ContainerRedpanda = "runsafe-redpanda-1"
	ContainerPostgres = "runsafe-postgres-1"
	ContainerConnect  = "runsafe-connect-1"
	ContainerFetcher  = "runsafe-fetcher-1"
)

// DockerAction stops, starts or restarts a container through the Docker Engine API on /var/run/docker.sock.
// The socket is only mounted into the test container for e2e runs (DGO_DOCKER_SOCKET=1 with scripts/dgo).
func DockerAction(t *testing.T, action, container string) {
	t.Helper()
	client := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", "/var/run/docker.sock")
			},
		},
	}
	url := fmt.Sprintf("http://docker/containers/%s/%s", container, action)
	if action == "stop" || action == "restart" {
		url += "?t=15"
	}
	req, _ := http.NewRequest(http.MethodPost, url, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("docker %s %s: %v", action, container, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotModified {
		t.Fatalf("docker %s %s: status %d: %s", action, container, resp.StatusCode, body)
	}
}
