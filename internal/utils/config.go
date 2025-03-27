package utils

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"runtime"
	"strings"

	"github.com/spf13/viper"
)

type CatalogSchemaConfig struct {
	Catalog string `mapstructure:"dbx_catalog" json:"catalog,omitempty"`
	Schema  string `mapstructure:"dbx_schema" json:"schema,omitempty"`
}

type Config struct {
	DbxHost              string                `mapstructure:"dbx_host"`
	DbxToken             string                `mapstructure:"dbx_token"`
	DbxClusterId         string                `mapstructure:"dbx_cluster_id"`
	DbxRunAs             string                `mapstructure:"dbx_run_as"`
	DbxSchemas           []CatalogSchemaConfig `mapstructure:"dbx_schemas"`
	DbxMaxActiveScanJobs string                `mapstructure:"dbx_max_active_scan_jobs"`
	DbxPollingQuartzCron string                `mapstructure:"dbx_polling_quartz_cron"`
	HlApiKeyName         string                `mapstructure:"hl_api_key_name"`
	HlClientID           string                `mapstructure:"hl_client_id"`
	HlClientSecret       string                `mapstructure:"hl_client_secret"`
	HlApiUrl             string                `mapstructure:"hl_api_url"`
	HlConsoleUrl         string                `mapstructure:"hl_console_url"`
}

// ConfigNotFound is a custom error type for configuration not found errors
type ConfigNotFound struct {
	Message string
}

func (e *ConfigNotFound) Error() string {
	return e.Message
}

// InitConfig reads in the configuration file and returns a Config object
func InitConfig() (*Config, error) {
	viper.SetConfigName("hldbx") // Config file name (without extension)
	viper.SetConfigType("yaml")  // Config file format

	// Determine the home directory based on the operating system
	homeDir := os.Getenv("HOME")
	if runtime.GOOS == "windows" {
		homeDir = os.Getenv("USERPROFILE")
	}

	// Look for the config file in the .hl directory under the home directory
	viper.AddConfigPath(fmt.Sprintf("%s/.hl", homeDir))

	// Read and unmarshal the config file
	if err := viper.ReadInConfig(); err != nil {
		return nil, &ConfigNotFound{Message: "no config file found"}
	}
	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode into struct, %v", err)
	}

	return &config, nil
}

func (c *Config) UsesEnterpriseModelScanner() bool {
	// determine if user is configuring for an enterprise scanner i.e. not a hiddenlayer.ai API url
	hlApi, err := url.Parse(c.HlApiUrl)
	if err != nil {
		log.Fatalf("Error parsing HiddenLayer API URL: %v", err)
	}
	return !strings.HasSuffix(hlApi.Hostname(), ".hiddenlayer.ai")
}

// For testing only. Requires switching the file to the main package.
//func main() {
//	config, err := InitConfig()
//	if err != nil {
//		fmt.Printf("Error initializing config: %v\n", err)
//		return
//	}
//
//	// Use the config as needed
//	fmt.Printf("Configuration loaded successfully:\n")
//	fmt.Printf("hl_api_id: %s\n", config.HlApiId)
//	fmt.Printf("hl_api_key: %s\n", config.HlApiKey)
//}
