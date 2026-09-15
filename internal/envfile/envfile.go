// Package envfile loads optional local development environment files.
package envfile

import (
	"os"

	"github.com/joho/godotenv"
)

// Load reads path into the process environment when the file exists. Existing
// process variables are preserved, so explicit environment values take
// precedence over values from the local file. A missing file is not an error;
// malformed or unreadable files are returned to the caller.
func Load(path string) error {
	if err := godotenv.Load(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
