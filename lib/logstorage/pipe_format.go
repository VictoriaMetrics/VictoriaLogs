package logstorage

import (
	"encoding/base64"
	"fmt"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
	"github.com/valyala/quicktemplate"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// pipeFormat processes '| format ...' pipe.
//
// See https://docs.victoriametrics.com/victorialogs/logsql/#format-pipe
type pipeFormat struct {
	cases []pipeFormatCase

	isSwitch bool

	resultField string

	keepOriginalFields bool
	skipEmptyResults   bool
}

type pipeFormatCase struct {
	// iff is an optional filter for skipping the format case
	iff *ifFilter

	formatStr string
	steps     []patternStep
}

func (c *pipeFormatCase) String() string {
	if c.iff == nil {
		return "default " + quoteTokenIfNeeded(c.formatStr)
	}
	return fmt.Sprintf("case (%s) %s", c.iff.f.String(), quoteTokenIfNeeded(c.formatStr))
}

func pipeFormatCasesString(cases []pipeFormatCase, isSwitch bool) string {
	if !isSwitch {
		c := &cases[0]
		s := ""
		if c.iff != nil {
			s += c.iff.String() + " "
		}
		return s + quoteTokenIfNeeded(c.formatStr)
	}
	a := make([]string, len(cases))
	for i := range cases {
		a[i] = cases[i].String()
	}
	return "switch(" + strings.Join(a, " ") + ")"
}

func (pf *pipeFormat) String() string {
	s := "format " + pipeFormatCasesString(pf.cases, pf.isSwitch)
	if !isMsgFieldName(pf.resultField) {
		s += " as " + quoteTokenIfNeeded(pf.resultField)
	}
	if pf.keepOriginalFields {
		s += " keep_original_fields"
	}
	if pf.skipEmptyResults {
		s += " skip_empty_results"
	}
	return s
}

func (pf *pipeFormat) splitToRemoteAndLocal(_ int64) (pipe, []pipe) {
	return pf, nil
}

func (pf *pipeFormat) canLiveTail() bool {
	return true
}

func (pf *pipeFormat) canReturnLastNResults() bool {
	return pf.resultField != "_time"
}

func (pf *pipeFormat) isFixedOutputFieldsOrder() bool {
	return false
}

func (pf *pipeFormat) updateNeededFields(f *prefixfilter.Filter) {
	if !f.MatchString(pf.resultField) {
		return
	}

	// Only the last case may have no 'if' filter
	lastCase := &pf.cases[len(pf.cases)-1]
	if shouldDenyOverwrittenField(lastCase.iff, pf.keepOriginalFields, pf.skipEmptyResults) {
		f.AddDenyFilter(pf.resultField)
	}
	for i := range pf.cases {
		c := &pf.cases[i]
		if c.iff != nil {
			f.AddAllowFilters(c.iff.allowFilters)
		}
		for _, step := range c.steps {
			if step.field != "" {
				f.AddAllowFilter(step.field)
			}
		}
	}
}

func (pf *pipeFormat) hasFilterInWithQuery() bool {
	for i := range pf.cases {
		if pf.cases[i].iff.hasFilterInWithQuery() {
			return true
		}
	}
	return false
}

func (pf *pipeFormat) initFilterInValues(cache *inValuesCache, getFieldValues getFieldValuesFunc) (pipe, error) {
	casesNew := make([]pipeFormatCase, len(pf.cases))
	for i := range pf.cases {
		c := &pf.cases[i]
		iffNew, err := c.iff.initFilterInValues(cache, getFieldValues)
		if err != nil {
			return nil, err
		}
		cNew := *c
		cNew.iff = iffNew
		casesNew[i] = cNew
	}
	pfNew := *pf
	pfNew.cases = casesNew
	return &pfNew, nil
}

func (pf *pipeFormat) visitSubqueries(visitFunc func(q *Query)) {
	for i := range pf.cases {
		pf.cases[i].iff.visitSubqueries(visitFunc)
	}
}

func (pf *pipeFormat) newPipeProcessor(_ int, _ <-chan struct{}, _ func(), ppNext pipeProcessor) pipeProcessor {
	pfp := &pipeFormatProcessor{
		pf:     pf,
		ppNext: ppNext,
	}
	pfp.shards.Init = func(shard *pipeFormatProcessorShard) {
		shard.bms = make([]bitmap, len(pf.cases))
	}
	return pfp
}

type pipeFormatProcessor struct {
	pf     *pipeFormat
	ppNext pipeProcessor

	shards atomicutil.Slice[pipeFormatProcessorShard]
}

type pipeFormatProcessorShard struct {
	bms []bitmap

	a  arena
	rc resultColumn
}

func (pfp *pipeFormatProcessor) writeBlock(workerID uint, br *blockResult) {
	if br.rowsLen == 0 {
		return
	}

	shard := pfp.shards.Get(workerID)
	pf := pfp.pf

	hasMatchingCases := false
	for i := range pf.cases {
		iff := pf.cases[i].iff
		if iff == nil {
			hasMatchingCases = true
			continue
		}
		bm := &shard.bms[i]
		bm.init(br.rowsLen)
		bm.setBits()
		iff.f.applyToBlockResult(br, bm)
		if !bm.isZero() {
			hasMatchingCases = true
		}
	}
	if !hasMatchingCases {
		pfp.ppNext.writeBlock(workerID, br)
		return
	}

	shard.rc.name = pf.resultField

	resultColumn := br.getColumnByName(pf.resultField)
	for rowIdx := range br.rowsLen {
		v := ""
		if c := pf.getMatchingCase(shard.bms, rowIdx); c != nil {
			v = shard.formatRow(c, br, rowIdx)
			if v == "" && pf.skipEmptyResults || pf.keepOriginalFields {
				if vOrig := resultColumn.getValueAtRow(br, rowIdx); vOrig != "" {
					v = vOrig
				}
			}
		} else {
			v = resultColumn.getValueAtRow(br, rowIdx)
		}
		shard.rc.addValue(v)
	}

	br.addResultColumn(shard.rc)
	pfp.ppNext.writeBlock(workerID, br)

	shard.a.reset()
	shard.rc.reset()
}

func (pfp *pipeFormatProcessor) flush() error {
	return nil
}

func (pf *pipeFormat) getMatchingCase(bms []bitmap, rowIdx int) *pipeFormatCase {
	for i := range pf.cases {
		c := &pf.cases[i]
		if c.iff == nil || bms[i].isSetBit(rowIdx) {
			return c
		}
	}
	return nil
}

func (shard *pipeFormatProcessorShard) formatRow(pfc *pipeFormatCase, br *blockResult, rowIdx int) string {
	b := shard.a.b
	bLen := len(b)
	for _, step := range pfc.steps {
		b = append(b, step.prefix...)
		if step.field == "" {
			continue
		}

		c := br.getColumnByName(step.field)
		v := c.getValueAtRow(br, rowIdx)
		switch step.fieldOpt {
		case "base64decode":
			result, ok := appendBase64Decode(b, v)
			if !ok {
				b = append(b, v...)
			} else {
				b = result
			}
		case "base64encode":
			b = appendBase64Encode(b, v)
		case "duration":
			nsecs, ok := tryParseInt64(v)
			if !ok {
				b = append(b, v...)
			} else {
				b = marshalDurationString(b, nsecs)
			}
		case "duration_seconds":
			nsecs, ok := tryParseDuration(v)
			if !ok {
				b = append(b, v...)
			} else {
				secs := float64(nsecs) / 1e9
				b = marshalFloat64String(b, secs)
			}
		case "hexdecode":
			b = appendHexDecode(b, v)
		case "hexencode":
			b = appendHexEncode(b, v)
		case "hexnumdecode":
			b = appendHexUint64Decode(b, v)
		case "hexnumencode":
			n, ok := tryParseUint64(v)
			if !ok {
				b = append(b, v...)
			} else {
				b = appendHexUint64Encode(b, n)
			}
		case "ipv4":
			ipNum, ok := tryParseUint64(v)
			if !ok || ipNum > math.MaxUint32 {
				b = append(b, v...)
			} else {
				b = marshalIPv4String(b, uint32(ipNum))
			}
		case "lc":
			b = appendLowercase(b, v)
		case "time":
			nsecs, ok := timeutil.TryParseUnixTimestamp(v)
			if !ok {
				b = append(b, v...)
			} else {
				b = marshalTimestampRFC3339NanoString(b, nsecs)
			}
		case "q":
			b = quicktemplate.AppendJSONString(b, v, true)
		case "uc":
			b = appendUppercase(b, v)
		case "urldecode":
			b = appendURLDecode(b, v)
		case "urlencode":
			b = appendURLEncode(b, v)
		default:
			b = append(b, v...)
		}
	}
	shard.a.b = b

	return bytesutil.ToUnsafeString(b[bLen:])
}

func parsePipeFormat(lex *lexer) (pipe, error) {
	if !lex.isKeyword("format") {
		return nil, fmt.Errorf("unexpected token: %q; want %q", lex.token, "format")
	}
	lex.nextToken()

	cases, isSwitch, err := parsePipeFormatCases(lex)
	if err != nil {
		return nil, err
	}

	// parse optional 'as ...` part
	resultField := "_msg"
	if lex.isKeyword("as") {
		lex.nextToken()
		field, err := parseFieldName(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse result field after 'format %s as': %w", pipeFormatCasesString(cases, isSwitch), err)
		}
		resultField = field
	}

	keepOriginalFields := false
	skipEmptyResults := false
	switch {
	case lex.isKeyword("keep_original_fields"):
		lex.nextToken()
		keepOriginalFields = true
	case lex.isKeyword("skip_empty_results"):
		lex.nextToken()
		skipEmptyResults = true
	}

	pf := &pipeFormat{
		cases:              cases,
		isSwitch:           isSwitch,
		resultField:        resultField,
		keepOriginalFields: keepOriginalFields,
		skipEmptyResults:   skipEmptyResults,
	}

	return pf, nil
}

func parsePipeFormatCases(lex *lexer) ([]pipeFormatCase, bool, error) {
	if !lex.isKeyword("switch") {
		// parse optional if (...)
		var iff *ifFilter
		if lex.isKeyword("if") {
			f, err := parseIfFilter(lex)
			if err != nil {
				return nil, false, err
			}
			iff = f
		}
		c, err := parsePipeFormatCasePattern(lex, iff)
		if err != nil {
			return nil, false, err
		}
		return []pipeFormatCase{*c}, false, nil
	}
	lex.nextToken()

	if !lex.isKeyword("(") {
		return nil, false, fmt.Errorf("missing '(' after the 'switch'")
	}
	lex.nextToken()

	var cases []pipeFormatCase
	for !lex.isKeyword(")") {
		if lex.isEnd() {
			return nil, false, fmt.Errorf("missing ')' to close switch(...)")
		}

		if len(cases) > 0 && cases[len(cases)-1].iff == nil {
			return nil, false, fmt.Errorf("switch(...) cannot contain cases after the 'default'")
		}

		var iff *ifFilter
		switch {
		case lex.isKeyword("case"):
			f, err := parseIfFilter(lex)
			if err != nil {
				return nil, false, fmt.Errorf("cannot parse case filter: %w", err)
			}
			iff = f
		case lex.isKeyword("default"):
			lex.nextToken()
		default:
			return nil, false, fmt.Errorf("unexpected token inside switch(...): %q; want 'case' or 'default'", lex.token)
		}

		c, err := parsePipeFormatCasePattern(lex, iff)
		if err != nil {
			return nil, false, err
		}
		cases = append(cases, *c)
	}
	if len(cases) == 0 {
		return nil, false, fmt.Errorf("switch(...) must contain at least a single 'case' or 'default'")
	}
	lex.nextToken()

	return cases, true, nil
}

func parsePipeFormatCasePattern(lex *lexer, iff *ifFilter) (*pipeFormatCase, error) {
	formatStr, err := lex.nextCompoundToken()
	if err != nil {
		return nil, fmt.Errorf("cannot read 'format': %w", err)
	}
	steps, err := parsePatternSteps(formatStr)
	if err != nil {
		return nil, fmt.Errorf("cannot parse 'pattern' %q: %w", formatStr, err)
	}

	// Verify that all the fields mentioned in the format pattern do not end with '*'
	for _, step := range steps {
		if prefixfilter.IsWildcardFilter(step.field) {
			return nil, fmt.Errorf("'pattern' %q cannot contain wildcard fields like %q", formatStr, step.field)
		}
	}

	return &pipeFormatCase{
		iff:       iff,
		formatStr: formatStr,
		steps:     steps,
	}, nil
}

func appendUppercase(dst []byte, s string) []byte {
	for _, r := range s {
		r = unicode.ToUpper(r)
		dst = utf8.AppendRune(dst, r)
	}
	return dst
}

func appendLowercase(dst []byte, s string) []byte {
	for _, r := range s {
		r = unicode.ToLower(r)
		dst = utf8.AppendRune(dst, r)
	}
	return dst
}

func appendURLDecode(dst []byte, s string) []byte {
	for len(s) > 0 {
		n := strings.IndexAny(s, "%+")
		if n < 0 {
			return append(dst, s...)
		}
		dst = append(dst, s[:n]...)
		ch := s[n]
		s = s[n+1:]
		if ch == '+' {
			dst = append(dst, ' ')
			continue
		}
		if len(s) < 2 {
			dst = append(dst, '%')
			continue
		}
		hi, ok1 := unhexChar(s[0])
		lo, ok2 := unhexChar(s[1])
		if !ok1 || !ok2 {
			dst = append(dst, '%')
			continue
		}
		ch = (hi << 4) | lo
		dst = append(dst, ch)
		s = s[2:]
	}
	return dst
}

func appendURLEncode(dst []byte, s string) []byte {
	n := len(s)
	for i := range n {
		c := s[i]

		// See http://www.w3.org/TR/html5/forms.html#form-submission-algorithm
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '-' || c == '.' || c == '_' {
			dst = append(dst, c)
		} else {
			if c == ' ' {
				dst = append(dst, '+')
			} else {
				dst = append(dst, '%', hexCharUpper(c>>4), hexCharUpper(c&15))
			}
		}
	}
	return dst
}

func hexCharUpper(c byte) byte {
	if c < 10 {
		return '0' + c
	}
	return c - 10 + 'A'
}

func unhexChar(c byte) (byte, bool) {
	if c >= '0' && c <= '9' {
		return c - '0', true
	}
	if c >= 'A' && c <= 'F' {
		return c - 'A' + 10, true
	}
	if c >= 'a' && c <= 'f' {
		return c - 'a' + 10, true
	}
	return 0, false
}

func appendHexUint64Encode(dst []byte, n uint64) []byte {
	for shift := 60; shift >= 0; shift -= 4 {
		dst = append(dst, hexCharUpper(byte(n>>shift)&15))
	}
	return dst
}

func appendHexUint64Decode(dst []byte, s string) []byte {
	if len(s) > 16 {
		return append(dst, s...)
	}
	sOrig := s
	n := uint64(0)
	for len(s) > 0 {
		x, ok := unhexChar(s[0])
		if !ok {
			return append(dst, sOrig...)
		}
		n = (n << 4) | uint64(x)
		s = s[1:]
	}
	return marshalUint64String(dst, n)
}

func appendHexEncode(dst []byte, s string) []byte {
	for i := range len(s) {
		c := s[i]
		hi := hexCharUpper(c >> 4)
		lo := hexCharUpper(c & 15)
		dst = append(dst, hi, lo)
	}
	return dst
}

func appendHexDecode(dst []byte, s string) []byte {
	for len(s) >= 2 {
		hi, ok1 := unhexChar(s[0])
		lo, ok2 := unhexChar(s[1])
		if !ok1 || !ok2 {
			dst = append(dst, s[0], s[1])
		} else {
			ch := (hi << 4) | lo
			dst = append(dst, ch)
		}
		s = s[2:]
	}
	return append(dst, s...)
}

func appendBase64Encode(dst []byte, s string) []byte {
	return base64.StdEncoding.AppendEncode(dst, bytesutil.ToUnsafeBytes(s))
}

func appendBase64Decode(dst []byte, s string) ([]byte, bool) {
	result, err := base64.StdEncoding.AppendDecode(dst, bytesutil.ToUnsafeBytes(s))
	if err != nil {
		return dst, false
	}
	return result, true
}
