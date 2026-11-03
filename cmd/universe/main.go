// Command universe builds the export universe for the staging subscription:
// all IMOEX constituents, every OFZ traded on the TQOB board, the two nearest
// quarterly contracts of the liquid index futures plus the perpetual IMOEXF,
// and the most liquid money market funds.
//
// The list is checked into deploy/staging/universe-imoex-ofz.txt and rendered
// into EXPORT_ALL_TRADES by `make deploy-staging-env`. A stale list does not
// fail the exporter: it silently stops subscribing to instruments that entered
// the index or were issued after the last refresh, so refresh it before the
// quarterly rebalancing, after every futures expiry and on every new OFZ issue.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	imoexURL = "https://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?iss.meta=off&limit=100"
	ofzURL   = "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQOB/securities.json?iss.meta=off&limit=100"
	fortsURL = "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json?iss.meta=off&iss.only=securities&securities.columns=SECID,ASSETCODE,LASTTRADEDATE"
)

// Two quarterly contracts per asset cover the roll: liquidity moves to the
// next contract a week or two before expiry, and it is already subscribed.
var (
	indexFutureAssets = []string{"MIX", "RTS", "MXI"}
	// IMOEXF is perpetual: its code never rolls.
	fixedFutures = []string{"IMOEXF"}
	// Money market funds trade on TQBR; ISS has no "money market" flag, so the
	// list is fixed by turnover (ofz-market/1.7 spec §2).
	moneyMarketFunds = []string{"AKMM", "LQDT", "SBMM"}
)

// quarterlyMonths are the FORTS month codes of March, June, September, December.
const quarterlyMonths = "HMUZ"

// block is one ISS response block: column names plus rows aligned to them.
type block struct {
	Columns []string `json:"columns"`
	Data    [][]any  `json:"data"`
}

type httpClient struct {
	client *http.Client
}

func newHTTPClient() *httpClient {
	return &httpClient{client: &http.Client{Timeout: 30 * time.Second}}
}

func (c *httpClient) getJSON(url string, target any) error {
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set("User-Agent", "transaq-clickhouse-exporter-universe/1.0")
	response, err := c.client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: HTTP %d", url, response.StatusCode)
	}
	if err = json.NewDecoder(response.Body).Decode(target); err != nil {
		return fmt.Errorf("%s: decode: %w", url, err)
	}
	return nil
}

// columnValues returns every non-empty value of one column, looked up by name:
// ISS column order is not part of the contract.
func columnValues(b block, column string) ([]string, error) {
	index := -1
	for i, name := range b.Columns {
		if name == column {
			index = i
			break
		}
	}
	if index < 0 {
		return nil, fmt.Errorf("column %q not found in %v", column, b.Columns)
	}
	values := make([]string, 0, len(b.Data))
	for _, row := range b.Data {
		if index >= len(row) {
			continue
		}
		value, ok := row[index].(string)
		if !ok || value == "" {
			continue
		}
		values = append(values, strings.TrimSpace(value))
	}
	return values, nil
}

func imoexCodes(c *httpClient) ([]string, error) {
	var payload struct {
		Analytics block `json:"analytics"`
	}
	if err := c.getJSON(imoexURL, &payload); err != nil {
		return nil, err
	}
	return columnValues(payload.Analytics, "ticker")
}

func ofzCodes(c *httpClient) ([]string, error) {
	var payload struct {
		Securities block `json:"securities"`
	}
	if err := c.getJSON(ofzURL, &payload); err != nil {
		return nil, err
	}
	return columnValues(payload.Securities, "SECID")
}

// nearestFutures picks the n nearest live quarterly contracts of every asset,
// in asset order and then by expiry. Calendar spreads, monthly contracts and
// contracts past their last trading day are skipped; the expiry day itself
// still trades. An asset without a live contract is an error: the ISS answer
// is broken, not empty.
func nearestFutures(b block, assets []string, today time.Time, n int) ([]string, error) {
	index := make(map[string]int, len(b.Columns))
	for i, name := range b.Columns {
		index[name] = i
	}
	for _, column := range []string{"SECID", "ASSETCODE", "LASTTRADEDATE"} {
		if _, ok := index[column]; !ok {
			return nil, fmt.Errorf("column %q not found in %v", column, b.Columns)
		}
	}
	type contract struct {
		code   string
		expiry time.Time
	}
	byAsset := make(map[string][]contract)
	day := today.Format(time.DateOnly)
	for _, row := range b.Data {
		if len(row) < len(b.Columns) {
			continue
		}
		code, _ := row[index["SECID"]].(string)
		asset, _ := row[index["ASSETCODE"]].(string)
		last, _ := row[index["LASTTRADEDATE"]].(string)
		if len(code) != 4 || !strings.ContainsRune(quarterlyMonths, rune(code[2])) {
			continue
		}
		if last < day {
			continue
		}
		expiry, err := time.Parse(time.DateOnly, last)
		if err != nil {
			return nil, fmt.Errorf("%s: LASTTRADEDATE %q: %w", code, last, err)
		}
		byAsset[asset] = append(byAsset[asset], contract{code: code, expiry: expiry})
	}
	var codes []string
	for _, asset := range assets {
		contracts := byAsset[asset]
		if len(contracts) == 0 {
			return nil, fmt.Errorf("no live quarterly contract of %s", asset)
		}
		sort.Slice(contracts, func(i, j int) bool { return contracts[i].expiry.Before(contracts[j].expiry) })
		for i := 0; i < n && i < len(contracts); i++ {
			codes = append(codes, contracts[i].code)
		}
	}
	return codes, nil
}

func futuresCodes(c *httpClient, today time.Time) ([]string, error) {
	var payload struct {
		Securities block `json:"securities"`
	}
	if err := c.getJSON(fortsURL, &payload); err != nil {
		return nil, err
	}
	codes, err := nearestFutures(payload.Securities, indexFutureAssets, today, 2)
	if err != nil {
		return nil, err
	}
	return append(codes, fixedFutures...), nil
}

// buildUniverse merges both lists into a sorted set without duplicates.
func buildUniverse(lists ...[]string) []string {
	seen := make(map[string]struct{})
	for _, list := range lists {
		for _, code := range list {
			code = strings.TrimSpace(code)
			if code == "" {
				continue
			}
			seen[code] = struct{}{}
		}
	}
	codes := make([]string, 0, len(seen))
	for code := range seen {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

// readUniverse reads a checked-in list; blank lines are ignored.
func readUniverse(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var codes []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		codes = append(codes, line)
	}
	return codes, nil
}

func writeUniverse(path string, codes []string) error {
	body := strings.Join(codes, "\n") + "\n"
	return os.WriteFile(path, []byte(body), 0o644)
}

// diffUniverse reports added and removed codes between the checked-in and fresh lists.
func diffUniverse(current, fresh []string) (added, removed []string) {
	currentSet := make(map[string]struct{}, len(current))
	for _, code := range current {
		currentSet[code] = struct{}{}
	}
	freshSet := make(map[string]struct{}, len(fresh))
	for _, code := range fresh {
		freshSet[code] = struct{}{}
		if _, ok := currentSet[code]; !ok {
			added = append(added, code)
		}
	}
	for _, code := range current {
		if _, ok := freshSet[code]; !ok {
			removed = append(removed, code)
		}
	}
	return added, removed
}

func main() {
	out := flag.String("out", "deploy/staging/universe-imoex-ofz.txt", "checked-in universe file")
	write := flag.Bool("write", false, "rewrite the universe file from MOEX ISS")
	check := flag.Bool("check", false, "exit non-zero when the checked-in file is stale")
	flag.Parse()

	client := newHTTPClient()
	imoex, err := imoexCodes(client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read IMOEX constituents: %v\n", err)
		os.Exit(1)
	}
	ofz, err := ofzCodes(client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read OFZ board TQOB: %v\n", err)
		os.Exit(1)
	}
	futures, err := futuresCodes(client, time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "read FORTS index futures: %v\n", err)
		os.Exit(1)
	}
	fresh := buildUniverse(imoex, ofz, futures, moneyMarketFunds)
	if len(imoex) < 20 {
		fmt.Fprintf(os.Stderr, "IMOEX returned %d codes, refusing a universe that small\n", len(imoex))
		os.Exit(1)
	}
	if len(ofz) < 20 {
		fmt.Fprintf(os.Stderr, "TQOB returned %d codes, refusing a universe that small\n", len(ofz))
		os.Exit(1)
	}

	current, err := readUniverse(*out)
	if err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", *out, err)
		os.Exit(1)
	}
	added, removed := diffUniverse(current, fresh)
	fmt.Printf("IMOEX: %d, OFZ TQOB: %d, futures: %d, funds: %d, universe: %d; file %s: %d\n",
		len(imoex), len(ofz), len(futures), len(moneyMarketFunds), len(fresh), *out, len(current))
	for _, code := range added {
		fmt.Printf("+ %s\n", code)
	}
	for _, code := range removed {
		fmt.Printf("- %s\n", code)
	}

	if *write {
		if err = writeUniverse(*out, fresh); err != nil {
			fmt.Fprintf(os.Stderr, "write %s: %v\n", *out, err)
			os.Exit(1)
		}
		fmt.Printf("wrote %s\n", *out)
		return
	}
	if *check && (len(added) > 0 || len(removed) > 0) {
		fmt.Fprintf(os.Stderr, "%s is stale: run make universe-refresh\n", *out)
		os.Exit(1)
	}
}
