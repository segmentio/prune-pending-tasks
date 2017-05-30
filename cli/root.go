package cmd

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/prune-pending-tasks/lib"
	"github.com/spf13/cobra"
)

var (
	cluster string
	age     time.Duration
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "prune-pending-tasks",
	Short: "cli tool to prune pending tasks",
	RunE:  root,
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	RootCmd.Flags().StringVarP(&cluster, "cluster", "c", "", "cluster to prune tasks from")
	RootCmd.Flags().DurationVarP(&age, "age", "a", time.Duration(time.Hour), "how old a pending task should be to get pruned")
}

func root(cmd *cobra.Command, args []string) error {
	if cluster == "" {
		return errors.New("Must set --cluster flag")
	}

	pruner := lib.NewPruner(cluster)

	pending, err := pruner.GetPendingTasks()
	if err != nil {
		return err
	}

	cutoff := time.Now().Add(-age)
	stopped, err := pruner.PruneTasks(pending, cutoff)
	if err != nil {
		return err
	}

	fmt.Println("Stopped tasks:")
	for _, task := range stopped {
		fmt.Printf("* %s", *task)
	}

	return nil
}
