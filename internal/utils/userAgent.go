package utils

import (
	"github.com/databricks/databricks-sdk-go/useragent"
)

func ConfigureHLUserAgent() {
	useragent.WithProduct("hiddenlayer-model-scanner", Version)
	useragent.WithPartner("HiddenLayer")
}
