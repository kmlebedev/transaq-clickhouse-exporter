package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	tcClient "github.com/kmlebedev/txmlconnector/client"
	"github.com/kmlebedev/txmlconnector/client/commands"
	pb "github.com/kmlebedev/txmlconnector/proto"
	"google.golang.org/grpc"
)

type fakeConnectServiceClient struct {
	mu       sync.Mutex
	requests []string
	sent     chan string
}

func newFakeConnectServiceClient() *fakeConnectServiceClient {
	return &fakeConnectServiceClient{sent: make(chan string, 16)}
}

func (fake *fakeConnectServiceClient) FetchResponseData(
	context.Context,
	*pb.DataRequest,
	...grpc.CallOption,
) (grpc.ServerStreamingClient[pb.DataResponse], error) {
	return nil, errors.New("not implemented by test fake")
}

func (fake *fakeConnectServiceClient) SendCommand(
	_ context.Context,
	request *pb.SendCommandRequest,
	_ ...grpc.CallOption,
) (*pb.SendCommandResponse, error) {
	fake.mu.Lock()
	fake.requests = append(fake.requests, request.GetMessage())
	fake.mu.Unlock()
	select {
	case fake.sent <- request.GetMessage():
	default:
	}
	return &pb.SendCommandResponse{Message: `<result success="true"/>`}, nil
}

func newTestTCClient(rpc pb.ConnectServiceClient) *tcClient.TCClient {
	return &tcClient.TCClient{
		Client:           rpc,
		ServerStatusChan: make(chan commands.ServerStatus, 8),
		ShutdownChannel:  make(chan bool, 1),
	}
}

func TestProcessTransaqRetriesDisconnectedTerminal(t *testing.T) {
	rpc := newFakeConnectServiceClient()
	client := newTestTCClient(rpc)
	client.ServerStatusChan <- commands.ServerStatus{Connected: "error"}

	processCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- processTransaq(processCtx, client, reconnectConfig{
			terminalRetryInterval: time.Millisecond,
			restore:               func(*tcClient.TCClient) error { return nil },
		})
	}()

	select {
	case request := <-rpc.sent:
		if !strings.Contains(request, `id="connect"`) {
			t.Fatalf("retry command = %q", request)
		}
	case <-time.After(time.Second):
		t.Fatal("TRANSAQ reconnect command was not sent")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("processTransaq error = %v", err)
	}
}

func TestProcessTransaqRestoresOncePerConnectedTransition(t *testing.T) {
	client := newTestTCClient(newFakeConnectServiceClient())
	restored := make(chan struct{}, 4)
	processCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- processTransaq(processCtx, client, reconnectConfig{
			terminalRetryInterval: time.Hour,
			restore: func(*tcClient.TCClient) error {
				restored <- struct{}{}
				return nil
			},
		})
	}()

	client.ServerStatusChan <- commands.ServerStatus{Connected: "true"}
	client.ServerStatusChan <- commands.ServerStatus{Connected: "true"}
	client.ServerStatusChan <- commands.ServerStatus{Connected: "false"}
	client.ServerStatusChan <- commands.ServerStatus{Connected: "true"}

	for restoreCount := 0; restoreCount < 2; restoreCount++ {
		select {
		case <-restored:
		case <-time.After(time.Second):
			t.Fatalf("subscription restore count = %d, want 2", restoreCount)
		}
	}
	select {
	case <-restored:
		t.Fatal("subscriptions were restored more than once for one connected state")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("processTransaq error = %v", err)
	}
}

func TestProcessTransaqRetriesSubscriptionRestoreWithoutReconnect(t *testing.T) {
	rpc := newFakeConnectServiceClient()
	client := newTestTCClient(rpc)
	restored := make(chan struct{}, 2)
	restoreAttempts := 0
	processCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- processTransaq(processCtx, client, reconnectConfig{
			terminalRetryInterval: time.Millisecond,
			restore: func(*tcClient.TCClient) error {
				restoreAttempts++
				restored <- struct{}{}
				if restoreAttempts == 1 {
					return errors.New("temporary restore failure")
				}
				return nil
			},
		})
	}()
	client.ServerStatusChan <- commands.ServerStatus{Connected: "true"}

	for attempt := 0; attempt < 2; attempt++ {
		select {
		case <-restored:
		case <-time.After(time.Second):
			t.Fatalf("restore attempts = %d, want 2", attempt)
		}
	}
	select {
	case request := <-rpc.sent:
		t.Fatalf("unexpected terminal reconnect while connected: %q", request)
	default:
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("processTransaq error = %v", err)
	}
}

func TestProcessTransaqReturnsWhenResponseStreamCloses(t *testing.T) {
	client := newTestTCClient(newFakeConnectServiceClient())
	client.ShutdownChannel <- true

	err := processTransaq(context.Background(), client, reconnectConfig{
		terminalRetryInterval: time.Hour,
		restore:               func(*tcClient.TCClient) error { return nil },
	})
	if !errors.Is(err, errResponseStreamClosed) {
		t.Fatalf("processTransaq error = %v", err)
	}
}

func TestSupervisorRecreatesClientAfterResponseStreamCloses(t *testing.T) {
	supervisorCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	created := 0
	factory := func() (*tcClient.TCClient, error) {
		created++
		client := newTestTCClient(newFakeConnectServiceClient())
		if created == 1 {
			client.ShutdownChannel <- true
		} else {
			cancel()
		}
		return client, nil
	}

	err := superviseTransaq(supervisorCtx, factory, reconnectConfig{
		terminalRetryInterval: time.Hour,
		sessionRetryMin:       time.Millisecond,
		sessionRetryMax:       2 * time.Millisecond,
		disconnectTimeout:     time.Second,
		restore:               func(*tcClient.TCClient) error { return nil },
	})
	if err != nil {
		t.Fatalf("superviseTransaq error = %v", err)
	}
	if created != 2 {
		t.Fatalf("created clients = %d, want 2", created)
	}
}
