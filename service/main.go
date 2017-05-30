package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/conf"
	"github.com/segmentio/kit/log"
	"github.com/segmentio/prune-pending-tasks/lib"
)

type config struct {
	Clusters []string      `conf:"c" help:"Clusters to monitor"`
	Age      time.Duration `conf:"a" help:"Age at which to prune pending tasks"`
}

var DefaultConfig = config{
	Clusters: []string{"megapool"},
	Age:      time.Hour,
}

func main() {
	conf.Load(&DefaultConfig)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for _, cluster := range DefaultConfig.Clusters {
		wg.Add(1)
		go PeriodicallyPrune(&wg, ctx, cluster, DefaultConfig.Age)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		log.Info("Receieved %s, shutting down", sig)
		cancel()
	}
	wg.Wait()
}

func PeriodicallyPrune(wg *sync.WaitGroup, ctx context.Context, cluster string, age time.Duration) {
	log.Info("Begging monitoring on %s", cluster)
	pruner := lib.NewPruner(cluster)

	ticker := time.Tick(30 * time.Minute)
	for {
		select {
		case tick := <-ticker:
			pending, err := pruner.GetPendingTasks()
			if err != nil {
				log.Errorf("failed to get pending tasks: %s", err)
			}

			cutoff := tick.Add(-age)
			stopped, err := pruner.PruneTasks(pending, cutoff)
			if err != nil {
				log.Errorf("failed to stop tasks: %s", err)
			}
			if len(stopped) > 0 {
				log.Info("successfully pruned %d tasks", len(stopped))
			} else {
				log.Info("no tasks to prune")
			}
		case <-ctx.Done():
			log.Info("stopping monitoring on %s", cluster)
			wg.Done()
			return
		}
	}
}
