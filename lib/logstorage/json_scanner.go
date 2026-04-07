package logstorage

import (
	"sync"

	"github.com/valyala/fastjson"
)

// JSONScanner scans all existing JSON messages in a string.
//
// See https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model
//
// Use GetJSONScanner() for obtaining the scanner.
type JSONScanner struct {
	commonJSON

	// s is used for fast JSON parsing
	s fastjson.Scanner

	// err contains parsing error
	err error
}

// GetJSONScanner returns JSONScanner ready to parse JSON lines.
//
// Return the parser to the pool when it is no longer needed by calling PutJSONScanner().
func GetJSONScanner() *JSONScanner {
	v := scannerPool.Get()
	if v == nil {
		return &JSONScanner{}
	}
	return v.(*JSONScanner)
}

// PutJSONScanner returns the parser to the pool.
//
// The parser cannot be used after returning to the pool.
func PutJSONScanner(s *JSONScanner) {
	s.reset()
	scannerPool.Put(s)
}

var scannerPool sync.Pool

func (s *JSONScanner) Init(msg []byte, preserveKeys []string, fieldPrefix string) {
	s.s.InitBytes(msg)
	s.init(preserveKeys, maxFieldNameSize, fieldPrefix)
}

func (s *JSONScanner) NextLogMessage() bool {
	s.resetKeepSettings()
	if !s.s.Next() {
		s.err = s.s.Error()
		return false
	}
	v := s.s.Value()
	o, err := v.Object()
	if err != nil {
		s.err = err
		return false
	}
	s.appendLogFields(o)
	return true
}

func (s *JSONScanner) Error() error {
	return s.err
}
