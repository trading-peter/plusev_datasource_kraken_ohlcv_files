package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	ex "github.com/plusev-terminal/go-plugin-common/datasrc/exchange"
	m "github.com/plusev-terminal/go-plugin-common/meta"
	"github.com/plusev-terminal/go-plugin-common/plugin"
	"github.com/plusev-terminal/go-plugin-common/trading"
)

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

// filenamePattern matches Kraken archive filenames like "XBTEUR_1.csv"
// Captures: full pair name (e.g. "XBTEUR")
var filenamePattern = regexp.MustCompile(`^([A-Z0-9.]+)_1\.csv$`)

// Known quote currencies in Kraken's naming (checked longest-first to avoid
// matching a prefix like "USD" when "USDT" is the actual quote).
var knownQuotes = []string{
	"PYUSD", "USDT", "USDC",
	"AUD", "CAD", "CHF", "DAI", "ETH", "EUR", "GBP", "JPY",
	"USD", "XBT", "DOT", "AED",
}

type KrakenOHLCVPlugin struct {
	config *plugin.ConfigStore
}

func (p *KrakenOHLCVPlugin) GetMeta() m.Meta {
	return m.Meta{
		PluginID:    "kraken-ohlcv-files",
		Name:        "Kraken Historical OHLCV Files",
		AppID:       "datasrc",
		Category:    "file",
		Description: "Serves OHLCV candle data from Kraken's downloadable historical CSV archives. Place the CSV files in the plugin's data folder.",
		Author:      "trading_peter",
		Version:     "v1.0.0",
		Repository:  "https://github.com/trading-peter/plusev_datasource_kraken_ohlcv_files",
		Tags:        []string{"kraken", "ohlcv", "csv", "historical", "file"},
		Contacts: []m.AuthorContact{
			{Kind: "x.com", Value: "https://x.com/trading_peter"},
		},
		Resources: m.ResourceAccess{
			FsWriteAccess: map[string]string{
				"data": "/data",
			},
		},
		Features: []string{},
	}
}

func (p *KrakenOHLCVPlugin) GetConfigFields() []plugin.ConfigField {
	// No configuration needed — operates purely on files in /data
	return []plugin.ConfigField{}
}

func (p *KrakenOHLCVPlugin) OnInit(config *plugin.ConfigStore) error {
	p.config = config
	return nil
}

func (p *KrakenOHLCVPlugin) OnShutdown() error {
	return nil
}

func (p *KrakenOHLCVPlugin) GetRateLimits() []plugin.RateLimit {
	return []plugin.RateLimit{
		{
			Command: ex.CMD_GET_MARKETS,
			Scope:   []plugin.RateLimitScope{plugin.RateLimitScopeIP},
			RPS:     10.0, // Conservative rate limit
			Burst:   1,
		},
		{
			Command: ex.CMD_GET_TIMEFRAMES,
			Scope:   []plugin.RateLimitScope{plugin.RateLimitScopeIP},
			RPS:     10.0,
			Burst:   1,
		},
		{
			Command: ex.CMD_GET_OHLCV,
			Scope:   []plugin.RateLimitScope{plugin.RateLimitScopeIP},
			RPS:     10.0,
			Burst:   1,
		},
	}
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
	markets, err := p.scanMarkets()
	if err != nil {
		return plugin.ErrorResponse(err)
	}
	return plugin.SuccessResponse(markets, time.Hour)
}

func (p *KrakenOHLCVPlugin) handleGetTimeframes(params map[string]any) plugin.Response {
	return plugin.SuccessResponse([]string{"1m"}, time.Hour*24)
}

func (p *KrakenOHLCVPlugin) handleGetOHLCV(params map[string]any) plugin.Response {
	ohlcvParams := ex.GetOHLCVParamsFromMap(params)

	if ohlcvParams.Market.Symbol == "" {
		return plugin.ErrorResponseMsg("market.symbol is required")
	}

	// Find the matching CSV file.
	// The symbol in the market should match the filename pattern.
	// Try exact match first, then try resolving base/quote to filename.
	csvPath, err := p.resolveCSVPath(ohlcvParams.Market)
	if err != nil {
		return plugin.ErrorResponse(err)
	}

	records, err := p.readOHLCV(csvPath, ohlcvParams.StartTime, ohlcvParams.EndTime, ohlcvParams.Limit)
	if err != nil {
		return plugin.ErrorResponse(err)
	}

	return plugin.SuccessResponse(records)
}

// ============================================================================
// File scanning / market discovery
// ============================================================================

func (p *KrakenOHLCVPlugin) scanMarkets() ([]trading.Market, error) {
	entries, err := os.ReadDir("/data")
	if err != nil {
		if os.IsNotExist(err) {
			return []trading.Market{}, nil
		}
		return nil, fmt.Errorf("failed to read data directory: %w", err)
	}

	var markets []trading.Market
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		matches := filenamePattern.FindStringSubmatch(name)
		if matches == nil {
			continue
		}

		pairStr := matches[1]
		base, quote := splitPair(pairStr)
		if base == "" || quote == "" {
			continue
		}

		markets = append(markets, trading.Market{
			Symbol: pairStr,
			Base:   base,
			Quote:  quote,
		})
	}

	sort.Slice(markets, func(i, j int) bool {
		return markets[i].Symbol < markets[j].Symbol
	})

	return markets, nil
}

// splitPair splits a concatenated Kraken pair like "XBTEUR" into base and quote.
func splitPair(pair string) (base, quote string) {
	for _, q := range knownQuotes {
		if strings.HasSuffix(pair, q) {
			b := pair[:len(pair)-len(q)]
			if b != "" {
				return b, q
			}
		}
	}
	return "", ""
}

// resolveCSVPath finds the CSV file for a given market.
func (p *KrakenOHLCVPlugin) resolveCSVPath(market trading.Market) (string, error) {
	// Try symbol directly: e.g. "XBTEUR" → "XBTEUR_1.csv"
	candidates := []string{
		filepath.Join("/data", market.Symbol+"_1.csv"),
	}

	// Also try base+quote concatenation if symbol differs
	if market.Base != "" && market.Quote != "" {
		bq := market.Base + market.Quote
		if bq != market.Symbol {
			candidates = append(candidates, filepath.Join("/data", bq+"_1.csv"))
		}
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("no CSV file found for market %s (tried: %s)", market.Symbol, strings.Join(candidates, ", "))
}

// ============================================================================
// CSV reading
// ============================================================================

// readOHLCV reads a Kraken archive CSV and returns OHLCVRecords within the time range.
// CSV format (no header): timestamp,open,high,low,close,volume,count
// Timestamp is unix seconds. File is sorted ascending by timestamp.
func (p *KrakenOHLCVPlugin) readOHLCV(path string, startTime, endTime *time.Time, limit int) ([]trading.OHLCVRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	var records []trading.OHLCVRecord

	for {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("CSV read error: %w", err)
		}

		if len(row) < 6 {
			continue
		}

		unixTs, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			continue
		}

		t := time.Unix(unixTs, 0).UTC()

		// Apply time filters
		if startTime != nil && t.Before(*startTime) {
			continue
		}
		if endTime != nil && t.After(*endTime) {
			break // File is sorted ascending, so we can stop here
		}

		records = append(records, trading.OHLCVRecord{
			OpenTime: unixTs, // Unix seconds (consistent with other datasrc plugins)
			Open:     row[1],
			High:     row[2],
			Low:      row[3],
			Close:    row[4],
			Volume:   row[5],
			IsClosed: true,
		})

		if limit > 0 && len(records) >= limit {
			break
		}
	}

	return records, nil
}
