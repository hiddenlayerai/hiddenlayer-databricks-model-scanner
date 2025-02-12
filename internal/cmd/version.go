package cmd

import (
	"fmt"
	"github.com/hiddenlayer-engineering/hl-databricks/internal/utils"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Prints the hldbx version",
	Long:  "Prints the version of the hldbx CLI tool.",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("hldbx version: %s\n", utils.Version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
