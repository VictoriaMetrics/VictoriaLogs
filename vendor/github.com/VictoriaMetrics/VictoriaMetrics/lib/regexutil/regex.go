package regexutil

import (
	"regexp"
	"regexp/syntax"
	"strings"
)

// Regex implements an optimized string matching for Go regex.
//
// The following regexs are optimized:
//
// - plain string such as "foobar"
// - alternate strings such as "foo|bar|baz"
// - prefix match such as "foo.*" or "foo.+"
// - substring match such as ".*foo.*" or ".+bar.+"
type Regex struct {
	// exprStr is the original expression.
	exprStr string

	// prefix contains literal prefix for regex.
	// For example, prefix="foo" for regex="foo(a|b)"
	prefix string

	// isOnlyPrefix is set to true if the regex contains only the prefix.
	isOnlyPrefix bool

	// isSuffixDotStar is set to true if suffix is ".*"
	isSuffixDotStar bool

	// isSuffixDotPlus is set to true if suffix is ".+"
	isSuffixDotPlus bool

	// substrDotStar contains literal string for regex suffix=".*string.*"
	substrDotStar string

	// substrDotPlus contains literal string for regex suffix=".+string.+"
	substrDotPlus string

	// orValues contains or values for the suffix regex.
	// For example, orValues contain ["foo","bar","baz"] for regex="foo|bar|baz"
	orValues []string

	// fallbackRe is used for regexps without a specialized fast path.
	fallbackRe *regexp.Regexp
}

// NewRegex returns Regex for the given expr.
func NewRegex(expr string) (*Regex, error) {
	if _, err := regexp.Compile(expr); err != nil {
		return nil, err
	}

	prefix, suffix := SimplifyRegex(expr)
	sre := mustParseRegexp(suffix)
	orValues := getOrValues(sre)
	isOnlyPrefix := len(orValues) == 1 && orValues[0] == ""
	isSuffixDotStar := isDotOp(sre, syntax.OpStar)
	isSuffixDotPlus := isDotOp(sre, syntax.OpPlus)
	substrDotStar := getSubstringLiteral(sre, syntax.OpStar)
	substrDotPlus := getSubstringLiteral(sre, syntax.OpPlus)

	fallbackExpr := suffix
	if len(prefix) > 0 {
		fallbackExpr = regexp.QuoteMeta(prefix) + "(?:" + suffix + ")"
	}
	// The fallbackExpr must be properly compiled, since it has been already checked above.
	// Otherwise it is a bug, which must be fixed.
	fallbackRe := regexp.MustCompile(fallbackExpr)

	r := &Regex{
		exprStr:         expr,
		prefix:          prefix,
		isOnlyPrefix:    isOnlyPrefix,
		isSuffixDotStar: isSuffixDotStar,
		isSuffixDotPlus: isSuffixDotPlus,
		substrDotStar:   substrDotStar,
		substrDotPlus:   substrDotPlus,
		orValues:        orValues,
		fallbackRe:      fallbackRe,
	}
	return r, nil
}

// MatchString returns true if s matches r.
func (r *Regex) MatchString(s string) bool {
	if r.isOnlyPrefix {
		if len(r.prefix) == 0 {
			return true
		}
		return strings.Contains(s, r.prefix)
	}

	if len(r.prefix) == 0 {
		return r.matchStringNoPrefix(s)
	}
	return r.matchStringWithPrefix(s)
}

// GetLiterals returns literals for r.
func (r *Regex) GetLiterals() []string {
	sre := mustParseRegexp(r.exprStr)
	for sre.Op == syntax.OpCapture {
		sre = sre.Sub[0]
	}

	v, ok := getLiteral(sre)
	if ok {
		return []string{v}
	}

	if sre.Op != syntax.OpConcat {
		return nil
	}

	var a []string
	for _, sub := range sre.Sub {
		v, ok := getLiteral(sub)
		if ok {
			a = append(a, v)
		}
	}
	return a
}

// String returns string representation for r
func (r *Regex) String() string {
	return r.exprStr
}

func (r *Regex) matchStringNoPrefix(s string) bool {
	if r.isSuffixDotStar {
		return true
	}
	if r.isSuffixDotPlus {
		return len(s) > 0
	}
	if r.substrDotStar != "" {
		// Fast path - r contains ".*someText.*"
		return strings.Contains(s, r.substrDotStar)
	}
	if r.substrDotPlus != "" {
		// Fast path - r contains ".+someText.+"
		n := strings.Index(s, r.substrDotPlus)
		return n > 0 && n+len(r.substrDotPlus) < len(s)
	}

	if len(r.orValues) == 0 {
		// Fall back to slow path by matching the suffix regexp.
		return r.fallbackRe.MatchString(s)
	}

	// Fast path - compare s to r.orValues
	for _, v := range r.orValues {
		if strings.Contains(s, v) {
			return true
		}
	}
	return false
}

func (r *Regex) matchStringWithPrefix(s string) bool {
	n := strings.Index(s, r.prefix)
	if n < 0 {
		// Fast path - s doesn't contain the needed prefix
		return false
	}
	sWithPrefix := s[n:]
	sNext := s[n+1:]
	s = s[n+len(r.prefix):]

	if r.isSuffixDotStar {
		return true
	}
	if r.isSuffixDotPlus {
		return len(s) > 0
	}
	if r.substrDotStar != "" {
		// Fast path - r contains ".*someText.*"
		return strings.Contains(s, r.substrDotStar)
	}
	if r.substrDotPlus != "" {
		// Fast path - r contains ".+someText.+"
		n := strings.Index(s, r.substrDotPlus)
		return n > 0 && n+len(r.substrDotPlus) < len(s)
	}
	if len(r.orValues) == 0 {
		// Fall back to slow path by matching the full regexp.
		return r.fallbackRe.MatchString(sWithPrefix)
	}

	for {
		// Fast path - compare s to r.orValues
		for _, v := range r.orValues {
			if strings.HasPrefix(s, v) {
				return true
			}
		}

		// Mismatch. Try again starting from the next char.
		s = sNext
		n := strings.Index(s, r.prefix)
		if n < 0 {
			// Fast path - s doesn't contain the needed prefix
			return false
		}
		sNext = s[n+1:]
		s = s[n+len(r.prefix):]
	}
}
