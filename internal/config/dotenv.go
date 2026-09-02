package config

import (
	"bufio"
	"os"
	"strings"
)

// loadDotEnv reads a .env file (if present) and sets each key=value pair
// as an environment variable — but only if not already set in the environment.
// Real environment variables always take precedence over the .env file.
// Missing .env file is not an error (returns nil).
func loadDotEnv(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no .env file — that's fine
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip blank lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Split on first '='
		idx := strings.IndexByte(line, '=')
		if idx == -1 {
			continue
		}

		key := strings.TrimSpace(line[:idx])
		value := strings.TrimSpace(line[idx+1:])

		// Strip optional surrounding quotes
		value = strings.Trim(value, `"'`)

		// Don't override real environment variables
		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, value)
		}
	}

	return scanner.Err()
}
