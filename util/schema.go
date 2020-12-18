package util

import (
	"strings"

	"github.com/riferrei/srclient"
)

func DetectSchemaFileType(file string) srclient.SchemaType {
	lastIndex := strings.LastIndex(file, ".")
	fileType := file[lastIndex:]
	return srclient.SchemaType(fileType)
}