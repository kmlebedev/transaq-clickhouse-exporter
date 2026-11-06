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
	mu           sync.Mutex
	commands     []string
	onCommand    func(string)
	failContains string
}

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
	request *pb.SendCommandRequest,
	_ ...grpc.CallOption,
) (*pb.SendCommandResponse, error) {
	message := request.GetMessage()
	fake.mu.Lock()
	fake.commands = append(fake.commands, message)
	onCommand := fake.onCommand
	failContains := fake.failContains
	fake.mu.Unlock()
	if onCommand != nil {
		onCommand(message)
	}
	if failContains != "" && strings.Contains(message, failContains) {
		return &pb.SendCommandResponse{
			Message: `<result success="false"><message>'secid' or 'seccode' and 'market' elements not found</message></result>`,
		}, nil
	}
	return &pb.SendCommandResponse{Message: `<result success="true"/>`}, nil
}

func (fake *fakeConnectServiceClient) lastCommand() string {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.commands) == 0 {
		return ""
	}
	return fake.commands[len(fake.commands)-1]
}

func newTestTCClient(rpc pb.ConnectServiceClient) *tcClient.TCClient {
	return &tcClient.TCClient{
		Client:           rpc,
		AllTradesChan:    make(chan commands.AllTrades, 16),
		QuotesChan:       make(chan commands.Quotes, 16),
		SecInfoChan:      make(chan commands.SecInfo, 16),
		SecInfoUpdChan:   make(chan commands.SecInfoUpd, 16),
		ServerStatusChan: make(chan commands.ServerStatus, 8),
		ShutdownChannel:  make(chan bool, 1),
		ResponseChannel:  make(chan string, 16),
	}
}

func newGRPCTestProvider(t *testing.T, client *tcClient.TCClient) Provider {
	t.Helper()
	previous := newGRPCClient
	newGRPCClient = func(tcClient.Options) (*tcClient.TCClient, error) {
		return client, nil
	}
	t.Cleanup(func() { newGRPCClient = previous })
	return &grpcProvider{}
}

func readyGRPCClient(rpc pb.ConnectServiceClient) *tcClient.TCClient {
	client := newTestTCClient(rpc)
	client.Data.Securities = commands.Securities{Items: []commands.Security{
		{SecId: 1, Active: "true", SecCode: "SBER", Board: "TQBR", Market: 1},
	}}
	client.Data.CandleKinds = commands.CandleKinds{Items: []commands.CandleKind{
		{ID: 1, Period: 60, Name: "1 мин"},
	}}
	client.ServerStatusChan <- commands.ServerStatus{Connected: "true"}
	return client
}

func TestGRPCProviderOpenBuildsCatalog(t *testing.T) {
	rpc := newFakeConnectServiceClient()
	client := readyGRPCClient(rpc)
	provider := newGRPCTestProvider(t, client)

	session, err := provider.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	catalog := session.(CatalogSource).Catalog()
	if len(catalog.Securities) != 1 || catalog.Securities[0].SecCode != "SBER" || !catalog.Securities[0].Active {
		t.Fatalf("catalog = %+v", catalog.Securities)
	}
	if len(catalog.CandleKinds) != 1 || catalog.CandleKinds[0].ID != 1 {
		t.Fatalf("candle kinds = %+v", catalog.CandleKinds)
	}
}

func TestGRPCProviderOpenTerminalDisconnect(t *testing.T) {
	client := newTestTCClient(newFakeConnectServiceClient())
	client.ServerStatusChan <- commands.ServerStatus{Connected: "error", Message: "denied"}
	provider := newGRPCTestProvider(t, client)

	_, err := provider.Open(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("error = %v", err)
	}
}

func TestGRPCProviderOpenEmptyCatalog(t *testing.T) {
	client := newTestTCClient(newFakeConnectServiceClient())
	client.ServerStatusChan <- commands.ServerStatus{Connected: "true"}
	provider := newGRPCTestProvider(t, client)

	_, err := provider.Open(context.Background())
	if err == nil || !strings.Contains(err.Error(), "catalog is empty") {
		t.Fatalf("error = %v", err)
	}
}

func TestGRPCSessionRunReturnsOnTerminalDisconnect(t *testing.T) {
	client := readyGRPCClient(newFakeConnectServiceClient())
	provider := newGRPCTestProvider(t, client)
	session, err := provider.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	client.ServerStatusChan <- commands.ServerStatus{Connected: "false"}
	err = session.Run(context.Background(), func(Event) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("Run error = %v", err)
	}
}

func TestGRPCSessionRunReturnsWhenStreamCloses(t *testing.T) {
	client := readyGRPCClient(newFakeConnectServiceClient())
	provider := newGRPCTestProvider(t, client)
	session, err := provider.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	client.ShutdownChannel <- true
	err = session.Run(context.Background(), func(Event) error { return nil })
	if !errors.Is(err, errResponseStreamClosed) {
		t.Fatalf("Run error = %v", err)
	}
}

func TestGRPCSessionSubscribeSendsSymbols(t *testing.T) {
	rpc := newFakeConnectServiceClient()
	client := readyGRPCClient(rpc)
	provider := newGRPCTestProvider(t, client)
	session, err := provider.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	err = session.(Subscriber).Subscribe(context.Background(), Subscription{
		AllTrades:  []Symbol{{Board: "TQBR", SecCode: "SBER"}},
		Quotations: []Symbol{{Board: "TQBR", SecCode: "SBER"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	command := rpc.lastCommand()
	if !strings.Contains(command, `id="subscribe"`) || !strings.Contains(command, "<alltrades>") ||
		!strings.Contains(command, "<secid>1</secid>") {
		t.Fatalf("subscribe command = %q", command)
	}
}

func TestGRPCSessionSecuritiesInfoIsolatesFailures(t *testing.T) {
	rpc := newFakeConnectServiceClient()
	client := newTestTCClient(rpc)
	client.Data.Securities = commands.Securities{Items: []commands.Security{
		{SecId: 1, Active: "true", SecCode: "SBER", Board: "TQBR", Market: 1},
		{SecId: 2, Active: "true", SecCode: "GAZP", Board: "TQBR", Market: 1},
	}}
	client.ServerStatusChan <- commands.ServerStatus{Connected: "true"}
	rpc.failContains = "<secid>2</secid>"
	rpc.onCommand = func(message string) {
		if strings.Contains(message, `id="get_securities_info"`) && strings.Contains(message, "<secid>1</secid>") {
			client.SecInfoChan <- commands.SecInfo{SecId: 1, SecCode: "SBER", FaceValue: 1000}
		}
	}
	provider := newGRPCTestProvider(t, client)
	session, err := provider.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	infos, failed, err := session.(SecuritiesInfoSource).SecuritiesInfo(context.Background(), []Symbol{
		{Board: "TQBR", SecCode: "SBER"},
		{Board: "TQBR", SecCode: "GAZP"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].SecID != 1 || infos[0].FaceValue != 1000 {
		t.Fatalf("infos = %+v", infos)
	}
	if failed != 1 {
		t.Fatalf("failed = %d, want 1", failed)
	}
}

func TestGRPCSessionHistoryWaitsForCandles(t *testing.T) {
	rpc := newFakeConnectServiceClient()
	client := readyGRPCClient(rpc)
	rpc.onCommand = func(message string) {
		if !strings.Contains(message, `id="gethistorydata"`) {
			return
		}
		client.Data.Candles = commands.Candles{
			SecId: 1, Board: "TQBR", SecCode: "SBER", Period: 1, Status: 1,
			Items: []commands.Candle{{Date: "14.08.2026 12:00:00", Open: 1, Close: 2, High: 3, Low: 0.5, Volume: 10}},
		}
		client.ResponseChannel <- "candles"
	}
	provider := newGRPCTestProvider(t, client)
	session, err := provider.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	candles, err := session.(HistorySource).History(context.Background(), HistoryRequest{
		Symbol: Symbol{Board: "TQBR", SecCode: "SBER"},
		Period: 1,
		Count:  10,
		Reset:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(candles) != 1 {
		t.Fatalf("candles = %+v", candles)
	}
	want := time.Date(2026, time.August, 14, 12, 0, 0, 0, moscowLocation)
	if !candles[0].Time.Equal(want) || candles[0].Close != 2 || candles[0].Volume != 10 {
		t.Fatalf("candle = %+v", candles[0])
	}
}

func TestGRPCSessionPositions(t *testing.T) {
	client := readyGRPCClient(newFakeConnectServiceClient())
	provider := newGRPCTestProvider(t, client)
	session, err := provider.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	client.Data.Positions = &commands.Positions{SecPositions: []commands.SecPosition{
		{SecId: 1, SecCode: "SBER"},
	}}
	client.ResponseChannel <- "positions"

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		positions := session.(PositionSource).Positions()
		if len(positions) == 1 && positions[0] == (Symbol{Board: "TQBR", SecCode: "SBER"}) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("positions = %+v", session.(PositionSource).Positions())
}

func TestGRPCSessionPushesMarketData(t *testing.T) {
	client := readyGRPCClient(newFakeConnectServiceClient())
	provider := newGRPCTestProvider(t, client)
	session, err := provider.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	client.AllTradesChan <- commands.AllTrades{Items: []commands.Trade{{
		SecId: 1, SecCode: "SBER", Board: "TQBR", TradeNo: 1,
		Time: "14.08.2026 12:00:00", Price: 100, Quantity: 2, BuySell: "B",
	}}}
	client.QuotesChan <- commands.Quotes{Items: []commands.Quote{{
		SecId: 1, SecCode: "SBER", Board: "TQBR", Price: 100, Buy: 1,
	}}}
	client.Data.Quotations = commands.Quotations{Items: []commands.Quotation{{
		SecId: 1, SecCode: "SBER", Board: "TQBR", Time: "12:00:00", Last: 100, Quantity: 2,
	}}}
	client.ResponseChannel <- "quotations"

	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan Event, 3)
	go func() {
		_ = session.Run(ctx, func(event Event) error {
			events <- event
			return nil
		})
	}()

	seen := map[string]bool{}
	deadline := time.After(2 * time.Second)
	for len(seen) < 3 {
		select {
		case event := <-events:
			switch event.(type) {
			case TradeEvent:
				seen["trade"] = true
			case QuotesEvent:
				seen["quotes"] = true
			case QuotationEvent:
				seen["quotation"] = true
			}
		case <-deadline:
			t.Fatalf("events = %+v", seen)
		}
	}
	cancel()
}

func TestGRPCSessionConsumesPortfolioUpdates(t *testing.T) {
	rpc := newFakeConnectServiceClient()
	client := newTestTCClient(rpc)
	session := newGRPCSession(client)
	updates := make(chan tcClient.PortfolioUpdate, 1)
	session.startWithUpdates(updates)
	defer session.Close()

	updates <- tcClient.PortfolioUpdate{
		Kind: tcClient.UpdateSecurities,
		Securities: &commands.Securities{Items: []commands.Security{{
			SecId: 1, Active: "true", SecCode: "SBER", Board: "TQBR",
		}}},
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		catalog := session.Catalog()
		if len(catalog.Securities) == 1 {
			if catalog.Securities[0].SecCode != "SBER" {
				t.Fatalf("catalog = %+v", catalog.Securities)
			}
			if err := session.Subscribe(context.Background(), Subscription{
				AllTrades: []Symbol{{Board: "TQBR", SecCode: "SBER"}},
			}); err != nil {
				t.Fatalf("subscribe after catalog update: %v", err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("каталог из PortfolioUpdates не собран")
}
