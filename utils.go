package beanstalk

import (
	"bytes"
	"errors"
)

var errEntryNotFound = errors.New("Yaml entry not found")

// includesString checks if string _s_ is included in the slice of strings _a_.
func includesString(a []string, s string) bool {
	for _, v := range a {
		if v == s {
			return true
		}
	}

	return false
}

// yamlValue returns the value of a yaml entry.
func yamlValue(yaml []byte, field string) (string, error) {
	var idxl, idxr int

	bField := []byte(field + ": ")
	if idxl = bytes.Index(yaml, bField); idxl == -1 {
		return "", errEntryNotFound
	}

	idxl += len(bField)
	if idxr = bytes.Index(yaml[idxl:], []byte("\n")); idxr == -1 {
		return "", errEntryNotFound
	}

	return string(yaml[idxl : idxl+idxr]), nil
}
