package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	client "github.com/attestantio/go-eth2-client"
	"github.com/attestantio/go-eth2-client/auto"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/cornelk/hashmap"
	"github.com/dgraph-io/badger/v3"
	"github.com/goccy/go-json"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/paulbellamy/ratecounter"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

const (
	secondsPerSlot = 12

	// How many slots behind head to start scraping from.
	// NOTE: Delete database after changing this.
	scrapeSlots = 450 * 32 // 450 epochs (2 days)

	// How many slots to fetch at once.
	scrapeConcurrency = 16
)

var targets = map[string]string{
	"prater": "http://prater-standalone.stage.bloxinfra.com:3500",
	// "prater":  "http://localhost:3500",
	"sepolia": "http://hetz:5052",
}

var stores = hashmap.New[string, *Store]()

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	for network, nodeURL := range targets {
		networkStore, err := OpenStore(network)
		if err != nil {
			log.Fatal(err)
		}
		defer networkStore.Close()
		stores.Set(network, networkStore)

		go func(network, nodeURL string) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := scrape(ctx, networkStore, network, nodeURL); err != nil {
					log.Printf("scrape(%s): %s", network, err)
					time.Sleep(time.Second * 16)
				}
			}
		}(network, nodeURL)
	}

	e := echo.New()
	e.Pre(middleware.RemoveTrailingSlash())
	e.GET("/:network/:slot", func(c echo.Context) error {
		network := c.Param("network")
		hideAttestations := c.QueryParams().Has("hide-attestations")
		hideTransactions := c.QueryParams().Has("hide-transactions")
		slot, err := strconv.Atoi(c.Param("slot"))
		if err != nil {
			return err
		}
		store, ok := stores.Get(network)
		if !ok {
			return echo.NewHTTPError(http.StatusNotFound, "network not found")
		}
		block, err := store.Block(phase0.Slot(slot))
		if err == badger.ErrKeyNotFound {
			return echo.NewHTTPError(http.StatusNotFound, "block not scraped")
		}
		if err != nil {
			return err
		}
		if block == nil {
			return c.JSON(http.StatusNotFound, map[string]interface{}{
				"code":    404,
				"message": "block not found",
			})
		}
		var resp struct {
			Version string      `json:"version"`
			Data    interface{} `json:"data"`
		}
		resp.Version = strings.ToLower(block.Version.String())
		switch block.Version {
		case spec.DataVersionPhase0:
			resp.Data = block.Phase0
			if hideAttestations {
				block.Phase0.Message.Body.Attestations = nil
			}
		case spec.DataVersionAltair:
			resp.Data = block.Altair
			if hideAttestations {
				block.Altair.Message.Body.Attestations = nil
			}
		case spec.DataVersionBellatrix:
			resp.Data = block.Bellatrix
			if hideAttestations {
				block.Bellatrix.Message.Body.Attestations = nil
			}
			if hideTransactions {
				block.Bellatrix.Message.Body.ExecutionPayload.Transactions = nil
			}
		}

		// Encode faster with goccy/go-json.
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		c.Response().WriteHeader(http.StatusOK)
		err = json.NewEncoder(c.Response().Writer).Encode(resp)
		if err != nil {
			log.Printf("failed to encode JSON: %s", err)
			return err
		}
		return nil
	})
	e.GET("/:network", func(ctx echo.Context) error {
		network := ctx.Param("network")
		store, ok := stores.Get(network)
		if !ok {
			return echo.NewHTTPError(http.StatusNotFound, "network not found")
		}
		slots, blocks, err := store.Count()
		if err != nil {
			return err
		}
		return ctx.JSON(http.StatusOK, map[string]interface{}{
			"slots":  slots,
			"blocks": blocks,
		})
	})
	go func() {
		if err := e.Start(":8080"); err != nil && err != http.ErrServerClosed {
			e.Logger.Fatal("shutting down the server")
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server with a timeout of 10 seconds.
	// Use a buffered channel to avoid missing signals as recommended for signal.Notify
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		e.Logger.Fatal(err)
	}
}

func scrape(ctx context.Context, store *Store, network, nodeURL string) error {
	// Connect to the node.
	svc, err := auto.New(ctx, auto.WithAddress(nodeURL), auto.WithLogLevel(zerolog.ErrorLevel))
	if err != nil {
		return errors.Wrap(err, "failed to connect to node")
	}

	// Get the genesis time.
	genesisTime, err := svc.(client.GenesisTimeProvider).GenesisTime(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get genesis time")
	}

	// Compute the slot to start scraping from.
	currentSlot := phase0.Slot(time.Since(genesisTime).Seconds() / secondsPerSlot)
	startSlot := currentSlot - scrapeSlots

	// Purge out of range slots.
	deleted, err := store.Purge(0, startSlot)
	if err != nil {
		return errors.Wrap(err, "failed to purge out of range slots")
	}
	log.Printf("%-10s purged %d outdated slots, starting from slot %d", network, deleted, startSlot)

	// Spawn goroutines to scrape the blocks.
	printTicker := time.NewTicker(time.Second)
	const rateInterval = 10 * time.Second
	rate := ratecounter.NewRateCounter(rateInterval)
	jobs := make(chan phase0.Slot)
	errs := make(chan error)
	for i := 0; i < scrapeConcurrency; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case slot := <-jobs:
					block, err := svc.(client.SignedBeaconBlockProvider).SignedBeaconBlock(ctx, fmt.Sprint(slot))
					if err != nil {
						// Hack to gracefully handle missing blocks from Prysm.
						notFound := false
						errString := err.Error()
						for _, s := range []string{
							"Could not get block from block ID: rpc error: code = NotFound",
							"rpc error: code = NotFound desc = Could not find requested block: signed beacon block can't be nil", // v2.1.0
							"Could not reconstruct full execution payload to create signed beacon block: block hash field in execution header",
						} {
							if strings.Contains(errString, s) {
								notFound = true
								break
							}
						}
						if !notFound {
							errs <- fmt.Errorf("failed to get block %d: %w", slot, err)
							return
						}
						block = nil
					}

					// Print progress.
					select {
					case <-printTicker.C:
						icon := "✅"
						if block == nil {
							icon = "❌"
						}
						slotsPerSecond := float64(rate.Rate()) / rateInterval.Seconds()
						eta := time.Duration(float64(currentSlot-slot)/slotsPerSecond) * time.Second
						log.Printf("%-10s %-8d %s %6.0f slots/s\teta: %s", network, slot, icon, slotsPerSecond, eta)
					default:
					}

					// Save it.
					err = store.SetBlock(slot, block)
					if err != nil {
						errs <- errors.Wrap(err, "failed to set block")
						return
					}
					rate.Incr(1)
				}
			}
		}()
	}

	// Scrape the blocks.
	for slot := startSlot; ; slot++ {
		// Skip slot if it's already in the store.
		exists, err := store.Filled(slot)
		if err != nil {
			return errors.Wrap(err, "failed to check if block exists")
		}
		if exists {
			continue
		}

		// Wait for next block to be at least 8 slots behind.
		futureSlot := genesisTime.Add(secondsPerSlot * time.Second * time.Duration(slot+8))
		if time.Now().Before(futureSlot) {
			select {
			case <-time.After(time.Until(futureSlot)):
			case err := <-errs:
				return err
			case <-ctx.Done():
				return nil
			}
		}

		// Get the next block.
		jobs <- slot
	}
}
