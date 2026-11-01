package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	ex "github.com/plusev-terminal/go-plugin-common/v2/datasrc/exchange"
	m "github.com/plusev-terminal/go-plugin-common/v2/meta"
	"github.com/plusev-terminal/go-plugin-common/v2/plugin"
	"github.com/plusev-terminal/go-plugin-common/v2/requester"
	rt "github.com/plusev-terminal/go-plugin-common/v2/requester/types"
	"github.com/plusev-terminal/go-plugin-common/v2/trading"
)

const dataDir = "/data"

// ============================================================================
// Main - Register the plugin
// ============================================================================

func init() {
	p := &KrakenOHLCVPlugin{}
	plugin.RegisterPlugin(p)
}

func main() {}

// ============================================================================
// Plugin Implementation
// ============================================================================

type KrakenOHLCVPlugin struct {
	config *plugin.ConfigStore
	req    *requester.Requester
}

func (p *KrakenOHLCVPlugin) GetMeta() m.Meta {
	return m.Meta{
		PluginID:    "kraken-ohlcv-files",
		Name:        "Kraken Historical OHLCV Files",
		AppID:       "datasrc",
		Category:    "file",
		Description: "Serves OHLCV candle data from Kraken's downloadable historical CSV archives. Extract Kraken_OHLCVT.zip into the plugin's data folder.",
		Author:      "trading_peter",
		Version:     "v1.0.0",
		Repository:  "https://github.com/trading-peter/plusev_datasource_kraken_ohlcv_files",
		Tags:        []string{"kraken", "ohlcv", "csv", "historical", "file"},
		Contacts: []m.AuthorContact{
			{Kind: "x.com", Value: "https://x.com/trading_peter"},
		},
		Resources: m.ResourceAccess{
			AllowedNetworkTargets: []m.NetworkTargetRule{
				{
					Pattern: "https://api.kraken.com/*",
					// Kraken Spot public endpoints: "Calling the public
					// endpoints at a frequency of 1 per second (or less)
					// would remain within the rate limits"; public endpoints
					// other than Trades and OHLC are rate limited by IP
					// address only. Source (retrieved 2026-10-31):
					// https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-
					// This plugin only calls /0/public/AssetPairs.
					Limits: []m.RateLimitRule{
						m.NewRateLimitRule("public", 1, time.Second).WithLabel("Public endpoints"),
					},
				},
			},
			FsWriteAccess: map[string]string{
				"data": "/data",
			},
		},
		Features: []string{},
	}
}

func (p *KrakenOHLCVPlugin) GetConfigFields() []plugin.ConfigField {
	return []plugin.ConfigField{}
}

func (p *KrakenOHLCVPlugin) OnInit(config *plugin.ConfigStore) error {
	p.config = config
	p.req = requester.NewRequester()
	return nil
}

func (p *KrakenOHLCVPlugin) OnShutdown() error {
	return nil
}

func (p *KrakenOHLCVPlugin) GetRateLimits() []plugin.RateLimit {
	// Empty on purpose: limits are declared on the manifest network target above;
	// an explicit empty array opts out of the legacy per-command limiter.
	return []plugin.RateLimit{}
}

func (p *KrakenOHLCVPlugin) RegisterCommands(router *plugin.CommandRouter) {
	router.Register(ex.CMD_GET_MARKETS, p.handleGetMarkets)
	router.Register(ex.CMD_GET_TIMEFRAMES, p.handleGetTimeframes)
	router.Register(ex.CMD_GET_OHLCV, p.handleGetOHLCV)
}

// ============================================================================
// Command Handlers
// ============================================================================

func (p *KrakenOHLCVPlugin) handleGetMarkets(params map[string]any) plugin.Response {
	markets, err := p.fetchMarkets()
	if err != nil {
		return plugin.ErrorResponse(err)
	}

	return plugin.SuccessResponse(markets, time.Hour)
}

func (p *KrakenOHLCVPlugin) handleGetTimeframes(params map[string]any) plugin.Response {
	// Timeframe values correspond to the minute-count suffixes in Kraken's CSV filenames.
	// e.g. XBTEUR_1.csv → 1m, XBTEUR_60.csv → 1h, XBTEUR_1440.csv → 1D
	return plugin.SuccessResponse([]string{"1m", "5m", "15m", "30m", "1h", "4h", "12h", "1D"}, time.Hour*24)
}

func (p *KrakenOHLCVPlugin) handleGetOHLCV(params map[string]any) plugin.Response {
	ohlcvParams := ex.GetOHLCVParamsFromMap(params)

	if ohlcvParams.Market.Symbol == "" {
		return plugin.ErrorResponseMsg("market.symbol is required")
	}

	if ohlcvParams.Timeframe == "" {
		return plugin.ErrorResponseMsg("timeframe is required")
	}

	tf, err := trading.TimeframeFromString(ohlcvParams.Timeframe)
	if err != nil {
		return plugin.ErrorResponseMsg("invalid timeframe: " + ohlcvParams.Timeframe)
	}

	entry, err := p.resolveCSVPath(ohlcvParams.Market.Symbol, tf.ToMinutes())
	if err != nil {
		return plugin.ErrorResponse(err)
	}

	f, err := os.Open(entry)
	if err != nil {
		return plugin.ErrorResponse(fmt.Errorf("failed to open CSV: %w", err))
	}
	defer f.Close()

	if ohlcvParams.StartTime != nil {
		seekBinarySearch(f, ohlcvParams.StartTime.Unix())
	}

	// When sorting descending, we must read the full window first so that
	// the limit is applied to the most-recent candles, not the oldest ones.
	readLimit := ohlcvParams.Limit
	if ohlcvParams.Sort == "desc" {
		readLimit = 0
	}

	records, err := p.readOHLCV(f, ohlcvParams.StartTime, ohlcvParams.EndTime, readLimit)
	if err != nil {
		return plugin.ErrorResponse(err)
	}

	if ohlcvParams.Sort == "desc" {
		for i, j := 0, len(records)-1; i < j; i, j = i+1, j-1 {
			records[i], records[j] = records[j], records[i]
		}
		if ohlcvParams.Limit > 0 && len(records) > ohlcvParams.Limit {
			records = records[:ohlcvParams.Limit]
		}
	}

	return plugin.SuccessResponse(records)
}

// ============================================================================
// Kraken REST API
// ============================================================================

type krakenAssetPairsResponse struct {
	Error  []string                       `json:"error"`
	Result map[string]krakenAssetPairInfo `json:"result"`
}

type krakenAssetPairInfo struct {
	Altname string `json:"altname"`
	WSName  string `json:"wsname"`
}

func (p *KrakenOHLCVPlugin) fetchMarkets() ([]trading.Market, error) {
	var apiResp krakenAssetPairsResponse

	_, err := p.req.Send(&rt.Request{
		URL:    "https://api.kraken.com/0/public/AssetPairs",
		Method: "GET",
	}, &apiResp)
	if err != nil {
		return nil, fmt.Errorf("kraken API request failed: %w", err)
	}

	if len(apiResp.Error) > 0 {
		return nil, fmt.Errorf("kraken API error: %s", strings.Join(apiResp.Error, ", "))
	}

	markets := make([]trading.Market, 0, len(apiResp.Result))

	for _, info := range apiResp.Result {
		// Skip dark pool entries (altname ends with ".d")
		if strings.HasSuffix(info.Altname, ".d") {
			continue
		}

		base, quote := parseWSName(info.WSName)
		if base == "" || quote == "" {
			continue
		}

		markets = append(markets, trading.Market{
			Symbol:    info.Altname,
			Label:     info.WSName,
			Base:      base,
			Quote:     quote,
			AssetType: trading.AssetTypeSpot,
		})
	}

	sort.Slice(markets, func(i, j int) bool {
		return markets[i].Symbol < markets[j].Symbol
	})

	return markets, nil
}

// parseWSName splits a Kraken wsname like "XBT/EUR" into base and quote.
func parseWSName(wsName string) (base, quote string) {
	parts := strings.SplitN(wsName, "/", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

// ============================================================================
// Binary Search Seek — O(log N) robust seek
// ============================================================================

// seekBinarySearch finds the exact byte offset of the target timestamp using
// a binary search over the file, then steps back a few kilobytes as a safety margin.
// This perfectly handles data gaps and varying density over years.
// O(log N) where N is bytes, ~25 seeks max for any file size.
func seekBinarySearch(f *os.File, targetTs int64) {
	fileSize, err := f.Seek(0, io.SeekEnd)
	if err != nil || fileSize == 0 {
		f.Seek(0, io.SeekStart)
		return
	}

	var low int64 = 0
	var high int64 = fileSize

	for low < high {
		mid := low + (high-low)/2
		f.Seek(mid, io.SeekStart)
		if mid > 0 {
			alignToNextNewline(f)
		}

		pos, _ := f.Seek(0, io.SeekCurrent)
		if pos >= fileSize {
			high = mid
			continue
		}

		var buf [128]byte
		n, _ := f.Read(buf[:])
		nl := bytes.IndexByte(buf[:n], '\n')
		if nl < 0 {
			high = mid
			continue
		}
		comma := bytes.IndexByte(buf[:nl], ',')
		if comma < 0 {
			high = mid
			continue
		}

		ts, ok := parseUnixBytes(buf[:comma])
		if !ok {
			high = mid
			continue
		}

		if ts < targetTs {
			low = pos + int64(nl) + 1
		} else {
			high = mid
		}
	}

	// Step back ~4KB to provide a safety margin before reading.
	safePos := low - 4096
	if safePos < 0 {
		safePos = 0
	}
	f.Seek(safePos, io.SeekStart)
	if safePos > 0 {
		alignToNextNewline(f)
	}
}

// parseUnixBytes parses a base-10 integer from a byte slice with zero allocations.
func parseUnixBytes(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var v int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		v = v*10 + int64(c-'0')
	}
	return v, true
}

// alignToNextNewline reads forward until after the next '\n' so the file
// position is at the start of a complete CSV row.
func alignToNextNewline(f *os.File) {
	var buf [512]byte
	for {
		nr, err := f.Read(buf[:])
		for i := 0; i < nr; i++ {
			if buf[i] == '\n' {
				// Seek back to just after this newline.
				f.Seek(-int64(nr-i-1), io.SeekCurrent)
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// resolveCSVPath finds the extracted CSV file for the given symbol and timeframe.
// Kraken's archive extracts into a subdirectory; we search one level deep.
func (p *KrakenOHLCVPlugin) resolveCSVPath(symbol string, minutes int) (string, error) {
	filename := fmt.Sprintf("%s_%d.csv", symbol, minutes)

	// Check directly in /data first.
	direct := filepath.Join(dataDir, filename)
	if _, err := os.Stat(direct); err == nil {
		return direct, nil
	}

	// Also search one subdirectory level (e.g. /data/master_q4/XBTEUR_1.csv).
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return "", fmt.Errorf("failed to read data directory: %w", err)
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		candidate := filepath.Join(dataDir, e.Name(), filename)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("no CSV file found for market %s at %dm (looked for: %s)", symbol, minutes, filename)
}

// ============================================================================
// CSV reading
// ============================================================================

// readOHLCV streams a Kraken CSV from r and returns records within the time range.
// CSV format (no header): timestamp,open,high,low,close,volume,count
// Timestamp is unix seconds. File is sorted ascending by timestamp.
func (p *KrakenOHLCVPlugin) readOHLCV(r io.Reader, startTime, endTime *time.Time, limit int) ([]trading.OHLCVRecord, error) {
	scanner := bufio.NewScanner(r)

	// Safely size buffer to handle long lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var records []trading.OHLCVRecord

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) < 6 {
			continue
		}

		// Fast zero-allocation timestamp check
		commaPos := bytes.IndexByte(line, ',')
		if commaPos < 0 {
			continue
		}

		unixTs, ok := parseUnixBytes(line[:commaPos])
		if !ok {
			continue
		}

		t := time.Unix(unixTs, 0).UTC()

		if startTime != nil && t.Before(*startTime) {
			continue
		}
		if endTime != nil && t.After(*endTime) {
			break // File is sorted ascending, so we can stop here
		}

		parts := bytes.Split(line, []byte{','})
		if len(parts) < 6 {
			continue
		}

		records = append(records, trading.OHLCVRecord{
			OpenTime: unixTs,
			Open:     string(parts[1]),
			High:     string(parts[2]),
			Low:      string(parts[3]),
			Close:    string(parts[4]),
			Volume:   string(parts[5]),
			IsClosed: true,
		})

		if limit > 0 && len(records) >= limit {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("CSV read error: %w", err)
	}

	return records, nil
}
