package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"golang.org/x/term"
)

const (
	keyUp = -(iota + 1)
	keyDown
	keyPgUp
	keyPgDn
	keyHome
	keyEnd
	keyNone
)

func readWithBuiltinPager(r io.Reader) error {
	lr := newLineReader(r)

	tty, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("cannot open tty: %w", err)
	}
	defer tty.Close()

	fd := int(tty.Fd())
	oldState, err := term.MakeRaw(fd)
	if err != nil {
		return fmt.Errorf("cannot enable raw terminal: %w", err)
	}
	defer term.Restore(fd, oldState)

	fmt.Fprint(tty, "\x1b[?1049h\x1b[?25l\x1b[?7l")
	defer fmt.Fprint(tty, "\x1b[?25h\x1b[?1049l\x1b[?7h")

	in := bufio.NewReader(tty)
	start := 0

	for {
		width, height, err := term.GetSize(fd)
		if err != nil {
			return fmt.Errorf("cannot get terminal size: %w", err)
		}
		if start < 0 {
			start = 0
		}
		end := start + height
		if lr.eof && end > lr.count() {
			start = lr.count() - height
			end = start + height
		}

		lines, err := lr.getLines(start, end)
		if err != nil {
			return fmt.Errorf("cannot get next lines: %w", err)
		}
		render(tty, lines, width)

		key, err := readKey(in)
		if err != nil {
			return fmt.Errorf("cannot read key: %w", err)
		}
		switch key {
		case 'q', 'Q':
			// Exit
			return nil
		case 'j', keyDown, '\r', '\n':
			// Move cursor down
			start++
		case 'k', keyUp:
			// Move cursor up
			start--
		case ' ', 'f', keyPgDn, ctrl('f'):
			// Page down
			start += height
		case 'b', keyPgUp, ctrl('b'):
			// Page up
			start -= height
		case 'd', ctrl('d'):
			// Half a page down
			start += height / 2
		case 'u', ctrl('u'):
			// Half a page up
			start -= height / 2
		case 'g', keyHome:
			// Move to start
			start = 0
		case 'G', keyEnd:
			// Move to end
			start = 0 // todo
		}
	}
}

type lineReader struct {
	sc         *bufio.Scanner
	eof        bool
	buf        []byte
	linesRange []bufRange
	linesBuf   []string
}

type bufRange struct {
	start int
	end   int
}

func newLineReader(r io.Reader) *lineReader {
	sc := bufio.NewScanner(r)
	return &lineReader{
		sc: sc,
	}
}

func (lr *lineReader) getLines(from, to int) ([]string, error) {
	if to < from {
		panic(fmt.Errorf("BUG: 'from' (%d) is greater that 'to' (%d)", from, to))
	}
	toRead := to - len(lr.linesRange)
	if toRead > 0 {
		if err := lr.readLines(toRead); err != nil {
			return nil, err
		}
	}
	ranges := lr.linesRange
	if to > len(ranges) {
		to = len(ranges)
	}
	buf := lr.buf
	clear(lr.linesBuf)
	linesBuf := lr.linesBuf[:0]
	for i := from; i < to; i++ {
		r := ranges[i]
		line := bytesutil.ToUnsafeString(buf[r.start:r.end])
		linesBuf = append(linesBuf, line)
	}
	lr.linesBuf = linesBuf
	return linesBuf, nil
}

func (lr *lineReader) readLines(n int) error {
	lines := lr.linesRange
	buf := lr.buf
	for range n {
		if !lr.sc.Scan() {
			lr.eof = true
			break
		}
		line := lr.sc.Bytes()
		start := len(buf)
		buf = append(buf, line...)
		end := len(buf)
		lines = append(lines, bufRange{start: start, end: end})
	}
	lr.buf = buf
	lr.linesRange = lines
	if err := lr.sc.Err(); err != nil {
		if isErrPipe(err) {
			err = nil
		}
		return err
	}
	return nil
}

func (lr *lineReader) count() int {
	return len(lr.linesRange)
}

func render(w io.Writer, lines []string, width int) {
	var b strings.Builder
	b.WriteString("\x1b[H")
	for _, line := range lines {
		b.WriteString(truncate(line, width))
		b.WriteString("\x1b[K\r\n")
	}
	io.WriteString(w, b.String())
}

func truncate(s string, width int) string {
	if width <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= width {
		return s
	}
	return string(runes[:width])
}

func readKey(in *bufio.Reader) (int, error) {
	r, _, err := in.ReadRune()
	if err != nil {
		return keyNone, err
	}
	if r != 0x1b {
		return int(r), nil
	}

	b1, err := in.ReadByte()
	if err != nil {
		return keyNone, err
	}
	if b1 != '[' && b1 != 'O' {
		return keyNone, nil
	}
	b2, err := in.ReadByte()
	if err != nil {
		return keyNone, err
	}

	switch b2 {
	case 'A':
		return keyUp, nil
	case 'B':
		return keyDown, nil
	case 'H':
		return keyHome, nil
	case 'F':
		return keyEnd, nil
	case '5', '6', '1', '4':
		tilde, err := in.ReadByte()
		if err != nil {
			return keyNone, err
		}
		if tilde != '~' {
			return keyNone, nil
		}
		switch b2 {
		case '5':
			return keyPgUp, nil
		case '6':
			return keyPgDn, nil
		case '1':
			return keyHome, nil
		case '4':
			return keyEnd, nil
		}
	}
	return keyNone, nil
}

func ctrl(c rune) int {
	return int(c) & 0x1f
}
