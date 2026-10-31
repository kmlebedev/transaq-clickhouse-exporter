package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	tcClient "github.com/kmlebedev/txmlconnector/client"
	"github.com/kmlebedev/txmlconnector/client/commands"
	pb "github.com/kmlebedev/txmlconnector/proto"
	"google.golang.org/grpc"
)

type fakeConnectServiceClient struct{}

func newFakeConnectServiceClient() *fakeConnectServiceClient {
	return &fakeConnectServiceClient{}
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
	_ *pb.SendCommandRequest,
	_ ...grpc.CallOption,
) (*pb.SendCommandResponse, error) {
	return &pb.SendCommandResponse{Message: `<result success="true"/>`}, nil
}

func newTestTCClient(rpc pb.ConnectServiceClient) *tcClient.TCClient {
	return &tcClient.TCClient{
		Client:           rpc,
		AllTradesChan:    make(chan commands.AllTrades, 16),
		SecInfoUpdChan:   make(chan commands.SecInfoUpd, 16),
		ServerStatusChan: make(chan commands.ServerStatus, 8),
		ShutdownChannel:  make(chan bool, 1),
	}
}

func TestProcessTransaqReturnsWhenTerminalDisconnected(t *testing.T) {
	client := newTestTCClient(newFakeConnectServiceClient())
	client.ServerStatusChan <- commands.ServerStatus{Connected: "error"}

	err := processTransaq(context.Background(), client, transaqSessionConfig{
		restore: func(*tcClient.TCClient) error { return nil },
	})
	if err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("processTransaq error = %v", err)
	}
}

func TestProcessTransaqRestoresOncePerSession(t *testing.T) {
	client := newTestTCClient(newFakeConnectServiceClient())
	restored := make(chan struct{}, 4)
	processCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- processTransaq(processCtx, client, transaqSessionConfig{
			restore: func(*tcClient.TCClient) error {
				restored <- struct{}{}
				return nil
			},
		})
	}()

	client.ServerStatusChan <- commands.ServerStatus{Connected: "true"}
	client.ServerStatusChan <- commands.ServerStatus{Connected: "true"}
	select {
	case <-restored:
	case <-time.After(time.Second):
		t.Fatal("subscriptions were not restored")
	}
	select {
	case <-restored:
		t.Fatal("subscriptions were restored more than once in one session")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("processTransaq error = %v", err)
	}
}

func TestProcessTransaqReturnsSubscriptionRestoreFailure(t *testing.T) {
	client := newTestTCClient(newFakeConnectServiceClient())
	restoreErr := errors.New("temporary restore failure")
	client.ServerStatusChan <- commands.ServerStatus{Connected: "true"}

	err := processTransaq(context.Background(), client, transaqSessionConfig{
		restore: func(*tcClient.TCClient) error { return restoreErr },
	})
	if !errors.Is(err, restoreErr) {
		t.Fatalf("processTransaq error = %v", err)
	}
}

func TestProcessTransaqDrainsEventsWhileRestoringSubscriptions(t *testing.T) {
	client := newTestTCClient(newFakeConnectServiceClient())
	restoreStarted := make(chan struct{})
	releaseRestore := make(chan struct{})
	processCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- processTransaq(processCtx, client, transaqSessionConfig{
			restore: func(*tcClient.TCClient) error {
				close(restoreStarted)
				<-releaseRestore
				return nil
			},
		})
	}()

	client.ServerStatusChan <- commands.ServerStatus{Connected: "true"}
	select {
	case <-restoreStarted:
	case <-time.After(time.Second):
		t.Fatal("subscription restore did not start")
	}

	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for index := 0; index < 256; index++ {
			client.SecInfoUpdChan <- commands.SecInfoUpd{SecId: index + 1}
		}
	}()
	drainedWhileRestoring := false
	select {
	case <-sent:
		drainedWhileRestoring = true
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseRestore)
	if !drainedWhileRestoring {
		select {
		case <-sent:
		case <-time.After(time.Second):
			t.Fatal("event producer remained blocked after subscription restore")
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("processTransaq error = %v", err)
	}
	if !drainedWhileRestoring {
		t.Fatal("TRANSAQ event channel filled while subscription restore was blocked")
	}
}

func TestProcessTransaqDrainsEventsWhileClickHouseWorkerIsSlow(t *testing.T) {
	client := newTestTCClient(newFakeConnectServiceClient())
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	processCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- processTransaq(processCtx, client, transaqSessionConfig{
			restore: func(*tcClient.TCClient) error { return nil },
			eventHandlers: transaqEventHandlers{
				allTrades: func(context.Context, commands.AllTrades) error {
					select {
					case <-handlerStarted:
					default:
						close(handlerStarted)
					}
					<-releaseHandler
					return nil
				},
			},
		})
	}()

	client.AllTradesChan <- commands.AllTrades{}
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("ClickHouse worker did not start")
	}

	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for index := 0; index < 256; index++ {
			client.AllTradesChan <- commands.AllTrades{}
		}
	}()
	select {
	case <-sent:
	case <-time.After(time.Second):
		close(releaseHandler)
		cancel()
		<-done
		t.Fatal("TRANSAQ event channel filled behind a slow ClickHouse worker")
	}

	close(releaseHandler)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("processTransaq error = %v", err)
	}
}

func TestProcessTransaqReturnsWhenResponseStreamCloses(t *testing.T) {
	client := newTestTCClient(newFakeConnectServiceClient())
	client.ShutdownChannel <- true

	err := processTransaq(context.Background(), client, transaqSessionConfig{
		restore: func(*tcClient.TCClient) error { return nil },
	})
	if !errors.Is(err, errResponseStreamClosed) {
		t.Fatalf("processTransaq error = %v", err)
	}
}

func TestRunTransaqDelegatesReconnectToClient(t *testing.T) {
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	created := 0
	factory := func() (*tcClient.TCClient, error) {
		created++
		client := newTestTCClient(newFakeConnectServiceClient())
		if created == 1 {
			client.ServerStatusChan <- commands.ServerStatus{Connected: "error"}
		} else {
			cancel()
		}
		return client, nil
	}

	err := runTransaq(
		runCtx,
		factory,
		transaqSessionConfig{restore: func(*tcClient.TCClient) error { return nil }},
		tcClient.ReconnectConfig{
			RetryMin:           time.Millisecond,
			RetryMax:           2 * time.Millisecond,
			SessionStableAfter: time.Hour,
			DisconnectTimeout:  time.Second,
		},
	)
	if err != nil {
		t.Fatalf("runTransaq error = %v", err)
	}
	if created != 2 {
		t.Fatalf("created clients = %d, want 2", created)
	}
}

type secInfoFailingServiceClient struct {
	fakeConnectServiceClient
	failSecCode string
	commands    []string
}

func (fake *secInfoFailingServiceClient) SendCommand(
	_ context.Context,
	request *pb.SendCommandRequest,
	_ ...grpc.CallOption,
) (*pb.SendCommandResponse, error) {
	fake.commands = append(fake.commands, request.GetMessage())
	if strings.Contains(request.GetMessage(), fake.failSecCode) {
		return &pb.SendCommandResponse{
			Message: `<result success="false"><message>'secid' or 'seccode' and 'market' elements not found</message></result>`,
		}, nil
	}
	return &pb.SendCommandResponse{Message: `<result success="true"/>`}, nil
}

func TestRequestSecuritiesInfoSkipsUnknownInstrument(t *testing.T) {
	rpc := &secInfoFailingServiceClient{failSecCode: "RU000A0JWYJ0"}
	client := newTestTCClient(rpc)

	failed := requestSecuritiesInfo(client, []secInfoKey{
		{SecID: 1, Market: 1, SecCode: "SU26238RMFS4"},
		{SecID: 1131, Market: 1, SecCode: "RU000A0JWYJ0"},
		{SecID: 3, Market: 1, SecCode: "SU26240RMFS0"},
	})
	if failed != 1 {
		t.Fatalf("failed = %d, want 1", failed)
	}
	if len(rpc.commands) != 3 {
		t.Fatalf("sent %d get_securities_info commands, want 3: one unknown instrument must not stop the rest", len(rpc.commands))
	}
}
