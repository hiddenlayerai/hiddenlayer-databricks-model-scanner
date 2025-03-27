package main

import (
	"github.com/hiddenlayer-engineering/hl-databricks/internal/cmd"
	"github.com/hiddenlayer-engineering/hl-databricks/internal/utils"
)

func main() {
	utils.ConfigureHLUserAgent()
	cmd.Execute()
}
