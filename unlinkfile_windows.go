package pipeat

import (
	"os"

	"golang.org/x/sys/windows"
)

func unlinkFile(file *os.File) (*os.File, error) {
	name := file.Name()
	err := file.Close()
	if err != nil {
		return file, err
	}
	// https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea
	handle, err := windows.CreateFile(
		windows.StringToUTF16Ptr(name),                            // File Name
		windows.GENERIC_READ|windows.GENERIC_WRITE|windows.DELETE, // Desired Access
		windows.FILE_SHARE_DELETE,                                 // Share Mode
		nil,                                                       // Security Attributes
		windows.TRUNCATE_EXISTING,                                 // Create Disposition
		windows.FILE_ATTRIBUTE_TEMPORARY|windows.FILE_FLAG_DELETE_ON_CLOSE, // Flags & Attributes
		0, // Template File
	)
	if err != nil {
		return file, err
	}
	file = os.NewFile(uintptr(handle), name)
	err = os.Remove(name)
	return file, err
}
