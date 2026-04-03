package logstorage

import (
	"testing"
)

func TestPipeTemplate(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	// Basic template extraction
	f("template(_msg)", [][]Field{
		{{"_msg", "user login from 1.2.3.4"}},
		{{"_msg", "user login from 5.6.7.8"}},
		{{"_msg", "other message"}},
	}, [][]Field{
		{{"_msg", "user login from <IP4>"}},
		{{"_msg", "other message"}},
	})

	// With hits
	f("template(_msg) with hits", [][]Field{
		{{"_msg", "user login from 1.2.3.4"}},
		{{"_msg", "user login from 5.6.7.8"}},
		{{"_msg", "other message"}},
	}, [][]Field{
		{{"_msg", "user login from <IP4>"}, {"hits", "2"}},
		{{"_msg", "other message"}, {"hits", "1"}},
	})

	// With limit
	f("template(_msg) with hits limit 1", [][]Field{
		{{"_msg", "user login from 1.2.3.4"}},
		{{"_msg", "user login from 5.6.7.8"}},
		{{"_msg", "other message"}},
	}, [][]Field{
		{{"_msg", "user login from <IP4>"}, {"hits", "2"}},
	})

	// Custom field
	f("template(foo) with hits", [][]Field{
		{{"foo", "error 404"}},
		{{"foo", "error 500"}},
		{{"foo", "ok"}},
	}, [][]Field{
		{{"foo", "error <N>"}, {"hits", "2"}},
		{{"foo", "ok"}, {"hits", "1"}},
	})
}
