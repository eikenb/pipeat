// +build !windows

package pipeat

import (
	"os"
)

func unlinkFile(file *os.File) (*os.File, error) {
	err := os.Remove(file.Name())
	return file, err
}
