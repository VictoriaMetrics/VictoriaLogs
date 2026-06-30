package logstorage

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
)

// TransformsProgram is a parsed VictoriaLogs transformations program,
// assembled from one or more content fragments via ParseAdditional and can be compiled into a Transformer.
type TransformsProgram struct {
	tps []*transformsProgram
}

// ParseTransformsProgram creates a new instance of TransformsProgram.
// Use ParseAdditional to parse additional programs.
func ParseTransformsProgram(s string) (*TransformsProgram, error) {
	tp := &TransformsProgram{}
	if err := tp.ParseAdditional(s); err != nil {
		return nil, err
	}
	return tp, nil
}

// ParseAdditional parses s as an additional program appended to the transformation pipeline.
// Note that named blocks are scoped to the program that declares them and are not visible to other programs.
func (tp *TransformsProgram) ParseAdditional(s string) error {
	tpNew, err := parseTransformsProgram(s)
	if err != nil {
		return err
	}
	tp.tps = append(tp.tps, tpNew)
	return nil
}

// Transformer can transform data according to the compiled TransformsProgram.
type Transformer struct {
	nextShard atomic.Uint64
	shards    []transformsProcessorShard
	pp        pipeProcessor
}

type transformsProcessorShard struct {
	mu sync.Mutex
	_  [atomicutil.CacheLineSize]byte
}

// NewTransformer creates a Transformer from tp that can transform incoming data according to the transformations program.
//
// flush is called for each batch of transformed log rows.
// The lr passed to flush is owned by the Transformer and is reused after flush returns,
// so flush must fully consume or copy it before returning.
func (tp *TransformsProgram) NewTransformer(flush func(lr *LogRows)) *Transformer {
	// Use twice CPU since workers have variable execution times,
	// causing faster goroutines to sit idle waiting for others.
	// This is necessary because the Transformer struct simulates workers that don't actually exist yet.
	//
	// TODO: add workers
	concurrency := cgroup.AvailableCPUs() * 2

	shards := make([]transformsProcessorShard, concurrency)
	storeResult := func(workerID uint, lr *LogRows) {
		if lr.RowsCount() == 0 {
			return
		}
		flush(lr)
	}
	noop := pipeProcessor(noopLogRowsPipeProcessorFunc(storeResult))

	ppSend := noop
	ppNext := noop
	for i := len(tp.tps) - 1; i >= 0; i-- {
		prog := tp.tps[i]
		ppNext = prog.newProcessor(concurrency, ppSend, ppNext)
	}

	return &Transformer{
		shards: shards,
		pp:     ppNext,
	}
}

type noopLogRowsPipeProcessorFunc func(workerID uint, lr *LogRows)

func (f noopLogRowsPipeProcessorFunc) writeBlock(workerID uint, br *blockResult) {
	lr := GetLogRows(nil, nil, nil, nil, "")
	defer PutLogRows(lr)
	lr.initFromBlockResult(br)
	f.writeLogRows(workerID, lr)
}

func (f noopLogRowsPipeProcessorFunc) writeLogRows(workerID uint, lr *LogRows) {
	f(workerID, lr)
}

func (noopLogRowsPipeProcessorFunc) flush() error {
	return nil
}

// Transform runs the compiled transformations over lr using the given workerID,
// and calls the flush callback for the result.
//
// It is safe to call Transform concurrently from worker goroutines.
func (t *Transformer) Transform(lr *LogRows) {
	// Emulate workers, as the writeBlock requires a workerID.
	workerID := t.nextShard.Add(1) % uint64(len(t.shards))
	shard := &t.shards[workerID]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	t.pp.writeLogRows(uint(workerID), lr)
}

// transformsProgram is a parsed VictoriaLogs transformations program.
type transformsProgram struct {
	// namedBlocks contains use-defined blocks to use via "do" keyword.
	namedBlocks []namedTransformBlock
	// root is the top-level transform executed for every log.
	root transform
}

type transform interface {
	// String return string representation of transform.
	String() string

	// newTransformProcessor builds the processor for this transform, chaining into ppNext.
	// ppSend is used to interrupt the execution of subsequent ppNext steps, redirecting logs directly to the final destination.
	// ppReturn is used to exit the execution of ppNext and resumes the main program flow.
	newTransformProcessor(concurrency int, ppSend, ppReturn, ppNext pipeProcessor) pipeProcessor
}

// parseTransformsProgram parses s into a transformsProgram.
func parseTransformsProgram(s string) (*transformsProgram, error) {
	lex := newLexer(s, time.Now().UnixNano())

	prog, err := parseTransformsProgramInternal(lex)
	if err != nil {
		return nil, fmt.Errorf("cannot parse transforms program: %w; context: [%s]", err, lex.context())
	}
	if err := prog.checkRecursion(); err != nil {
		return nil, err
	}
	if err := prog.initTransformsDo(); err != nil {
		return nil, err
	}
	if err := prog.checkTimeFilter(); err != nil {
		return nil, err
	}
	if err := prog.checkSubqueries(); err != nil {
		return nil, err
	}

	return prog, nil
}

func parseTransformsProgramInternal(lex *lexer) (*transformsProgram, error) {
	if lex.isKeyword("") {
		return nil, fmt.Errorf("missing transformations")
	}

	var namedBlocks []namedTransformBlock
	uniqNamedBlocks := make(map[string]struct{})

	var trs []transform
	for !lex.isKeyword("") {
		if lex.isKeyword("block") {
			tr, name, err := parseNamedTransformBlock(lex)
			if err != nil {
				return nil, err
			}
			if _, ok := uniqNamedBlocks[name]; ok {
				return nil, fmt.Errorf("duplicate named transformation block %q", name)
			}

			uniqNamedBlocks[name] = struct{}{}
			namedBlocks = append(namedBlocks, namedTransformBlock{
				name: name,
				body: tr,
			})
			continue
		}

		tr, err := parseGenericTransformBlockLine(lex)
		if err != nil {
			return nil, err
		}
		trs = append(trs, tr)
	}

	return &transformsProgram{
		namedBlocks: namedBlocks,
		root: &transformBlock{
			transforms: trs,
		},
	}, nil
}

func (tp *transformsProgram) String() string {
	var s string
	for _, nb := range tp.namedBlocks {
		s += fmt.Sprintf("block %s ", nb.name)
		s += nb.body.String()
		s += "\n"
	}

	// Override the top-level block implementation in order to remove curly braces and indentation.
	if tb, ok := tp.root.(*transformBlock); ok {
		for i, p := range tb.transforms {
			if i > 0 {
				s += "\n"
			}
			s += p.String()
		}
		return s
	}

	s += tp.root.String()
	return s
}

// newProcessor builds the processor chain for the whole program.
//
// At the root, ppSend, ppReturn and ppNext all point to the same downstream sink,
// so 'send' and 'return' at the top level are equivalent to falling through.
func (tp *transformsProgram) newProcessor(concurrency int, ppSend, ppNext pipeProcessor) pipeProcessor {
	return tp.root.newTransformProcessor(concurrency, ppSend, ppNext, ppNext)
}

// namedTransformBlock is a reusable block declared via 'block name {...}'.
type namedTransformBlock struct {
	name string
	body *transformBlock
}

func parseNamedTransformBlock(lex *lexer) (*transformBlock, string, error) {
	if !lex.isKeyword("block") {
		return nil, "", fmt.Errorf("expected keyword 'block', got %q", lex.token)
	}
	lex.nextToken()

	name, err := parseTransformBlockName(lex)
	if err != nil {
		return nil, "", err
	}

	tr, err := parseGenericTransformBlock(lex)
	if err != nil {
		return nil, "", err
	}
	return tr, name, nil
}

// transformsKeywords are reserved words that cannot be used as block names.
var transformsKeywords = []string{
	"if",
	"else",
	"send",
	"return",
	"drop",
	"block",
	"do",
}

func parseTransformBlockName(lex *lexer) (string, error) {
	if lex.isKeyword("") {
		return "", fmt.Errorf("unexpected end of block name")
	}
	if lex.isQuotedToken() {
		return "", fmt.Errorf("unexpected quotation mark in block declaration")
	}
	name := lex.token
	if slices.Contains(transformsKeywords, name) || isPipeName(name) {
		return "", fmt.Errorf("cannot declare block with name %q as it is a keyword", name)
	}
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return "", fmt.Errorf("invalid character %q in block declaration: %q", r, name)
		}
	}
	lex.nextToken()
	return name, nil
}

// transformBlock is a sequence of transforms wrapped in '{ }'.
type transformBlock struct {
	transforms []transform
}

// parseGenericTransformBlock parses a transformBlock wrapped in '{ }'.
func parseGenericTransformBlock(lex *lexer) (*transformBlock, error) {
	if !lex.isKeyword("{") {
		return nil, fmt.Errorf("unexpected token: %q; want %q", lex.token, "{")
	}
	lex.nextToken()

	var trs []transform
	for !lex.isKeyword("}", "") {
		tr, err := parseGenericTransformBlockLine(lex)
		if err != nil {
			return nil, err
		}
		trs = append(trs, tr)
	}
	if !lex.isKeyword("}") {
		return nil, fmt.Errorf("unexpected end of the transforms block")
	}
	lex.nextToken()

	return &transformBlock{
		transforms: trs,
	}, nil
}

// parseGenericTransformBlockLine parses a single line inside a transformBlock.
func parseGenericTransformBlockLine(lex *lexer) (transform, error) {
	if lex.isKeyword("block") {
		return nil, fmt.Errorf("declaring a named block inside another block is not allowed; declare the block at the top level of the file")
	}
	switch {
	case lex.isKeyword("do"):
		return parseTransformDo(lex)
	case lex.isKeyword("if"):
		return parseTransformIf(lex)
	case lex.isKeyword("send"):
		return parseTransformSend(lex)
	case lex.isKeyword("return"):
		return parseTransformReturn(lex)
	case lex.isKeyword("drop"):
		return parseTransformDrop(lex)
	default:
		return parseTransformPipes(lex)
	}
}

func (tb *transformBlock) String() string {
	if len(tb.transforms) == 0 {
		return "{}"
	}
	var s string
	s += "{"
	for _, p := range tb.transforms {
		s += "\n"
		s += addPrefixToLines(p.String(), "  ")
	}
	s += "\n"
	s += "}"
	return s
}

func (tb *transformBlock) newTransformProcessor(concurrency int, ppSend, ppReturn, ppNext pipeProcessor) pipeProcessor {
	for i := len(tb.transforms) - 1; i >= 0; i-- {
		tr := tb.transforms[i]
		ppNext = tr.newTransformProcessor(concurrency, ppSend, ppReturn, ppNext)
	}
	return &transformBlockProcessor{
		ppNext: ppNext,
	}
}

type transformBlockProcessor struct {
	ppNext pipeProcessor
}

func (tbp *transformBlockProcessor) writeBlock(workerID uint, br *blockResult) {
	tbp.ppNext.writeBlock(workerID, br)
}

func (tbp *transformBlockProcessor) writeLogRows(workerID uint, lr *LogRows) {
	tbp.ppNext.writeLogRows(workerID, lr)
}

func (tbp *transformBlockProcessor) flush() error {
	return tbp.ppNext.flush()
}

// transformDo references a named block declared elsewhere via 'do <blockName>'.
type transformDo struct {
	// blockName is the referenced block's blockName.
	blockName string

	// body contains initialized transformBlock to execute.
	// This field initializes after program is parsed.
	body *transformBlock
}

func parseTransformDo(lex *lexer) (*transformDo, error) {
	if !lex.isKeyword("do") {
		return nil, fmt.Errorf("unexpected token: %q; want %q", lex.token, "do")
	}
	lex.nextToken()

	name, err := parseTransformBlockName(lex)
	if err != nil {
		return nil, err
	}

	if !lex.isKeyword(";") {
		return nil, fmt.Errorf("expected 'do' to be ended with ';', got %q", lex.token)
	}
	lex.nextToken()

	return &transformDo{
		blockName: name,
	}, nil
}

func (td *transformDo) String() string {
	return fmt.Sprintf("do %s;", td.blockName)
}

func (td *transformDo) newTransformProcessor(concurrency int, ppSend, _, ppNext pipeProcessor) pipeProcessor {
	if td.body == nil {
		panic(fmt.Errorf("BUG: transform %q is not initialized", td.String()))
	}
	// Override ppReturn with ppNext, so the next 'return' call inside a named block will process ppNext.
	return td.body.newTransformProcessor(concurrency, ppSend, ppNext, ppNext)
}

// transformIf represents a single 'if ... else if ... else ...' chain.
type transformIf struct {
	// branches holds the 'if' branch followed by any 'else if' branches, in order.
	branches []*ifBranch
	// elseBody is an 'else' block, or nil if there is no 'else'.
	elseBody *transformBlock
}

// parseTransformIf parses transformIf.
//
// The chain is extended only through 'else if'.
// Two adjacent 'if' blocks with no 'else' between them are independent chains,
// so parsing stops after the first 'if', and the second one is parsed later as a separate transformIf.
func parseTransformIf(lex *lexer) (*transformIf, error) {
	if !lex.isKeyword("if") {
		return nil, fmt.Errorf("unexpected token: %q; want %q", lex.token, "if")
	}

	// Parse the start of the chain.
	var ifs []*ifBranch
	b, err := parseIfBranch(lex)
	if err != nil {
		return nil, err
	}
	ifs = append(ifs, b)

	var elseBody *transformBlock
	// Parse 'else if' chain
	for lex.isKeyword("else") {
		lex.nextToken()
		// Parse 'else if' block
		if lex.isKeyword("if") {
			b, err := parseIfBranch(lex)
			if err != nil {
				return nil, err
			}
			ifs = append(ifs, b)
			continue
		}
		// Parse 'else' block.
		b, err := parseGenericTransformBlock(lex)
		if err != nil {
			return nil, err
		}
		elseBody = b
		break
	}
	return &transformIf{
		branches: ifs,
		elseBody: elseBody,
	}, nil
}

func (ti *transformIf) String() string {
	var s string
	for i, ib := range ti.branches {
		if i > 0 {
			s += " else "
		}
		s += ib.String()
	}
	if ti.elseBody != nil {
		s += " else "
		s += ti.elseBody.String()
	}
	return s
}

func (ti *transformIf) newTransformProcessor(concurrency int, ppSend, ppReturn, ppNext pipeProcessor) pipeProcessor {
	// ppMatched is a next processor to use if a branch is matched.
	ppMatched := ppNext

	pp := ppNext
	if ti.elseBody != nil {
		pp = ti.elseBody.newTransformProcessor(concurrency, ppSend, ppReturn, pp)
	}
	for i := len(ti.branches) - 1; i >= 0; i-- {
		ib := ti.branches[i]
		pp = ib.newIfBranchProcessor(concurrency, ppSend, ppReturn, ppMatched, pp)
	}
	return pp
}

// ifBranch is a single 'if (filter) { body }' branch of a transformIf.
type ifBranch struct {
	// f is the condition the log must match for body to run.
	f    filter
	body *transformBlock
}

func (ib *ifBranch) String() string {
	return fmt.Sprintf("if (%s) %s", ib.f.String(), ib.body.String())
}

func (ib *ifBranch) newIfBranchProcessor(concurrency int, ppSend, ppReturn, ppNext, ppUnmatched pipeProcessor) pipeProcessor {
	ppMatched := ib.body.newTransformProcessor(concurrency, ppSend, ppReturn, ppNext)
	return &ifBranchProcessor{
		f:           ib.f,
		ppMatched:   ppMatched,
		ppUnmatched: ppUnmatched,
	}
}

type ifBranchProcessor struct {
	f           filter
	ppMatched   pipeProcessor
	ppUnmatched pipeProcessor

	shards atomicutil.Slice[ifBranchProcessorShard]
}

type ifBranchProcessorShard struct {
	bmMatched   bitmap
	bmUnmatched bitmap
	brMatched   blockResult
	brUnmatched blockResult
	lrMatched   LogRows
	lrUnmatched LogRows
}

func (ibp *ifBranchProcessor) writeBlock(workerID uint, br *blockResult) {
	if br.rowsLen == 0 {
		return
	}
	shard := ibp.shards.Get(workerID)
	bmMatched := &shard.bmMatched
	bmMatched.init(br.rowsLen)
	bmMatched.setBits()
	ibp.f.applyToBlockResult(br, bmMatched)
	// Fast path: all rows matched or unmatched.
	if bmMatched.isZero() {
		ibp.ppUnmatched.writeBlock(workerID, br)
		return
	}
	if bmMatched.areAllBitsSet() {
		ibp.ppMatched.writeBlock(workerID, br)
		return
	}

	// Slow path: split br by matched and unmatched.
	bmUnmatched := &shard.bmUnmatched
	bmUnmatched.init(bmMatched.bitsLen)
	bmUnmatched.setBits()
	bmUnmatched.andNot(bmMatched)

	brMatched := &shard.brMatched
	brUnmatched := &shard.brUnmatched
	brMatched.initFromFilterAllColumns(br, bmMatched)
	brUnmatched.initFromFilterAllColumns(br, bmUnmatched)

	ibp.ppMatched.writeBlock(workerID, brMatched)
	ibp.ppUnmatched.writeBlock(workerID, brUnmatched)
}

func (ibp *ifBranchProcessor) writeLogRows(workerID uint, lr *LogRows) {
	if lr.RowsCount() == 0 {
		return
	}

	// Find first matched and unmatched row
	firstMatched := -1
	firstUnmatched := -1
	f := ibp.f
	for i := range lr.RowsCount() {
		row := lr.mustGetRowFields(i)
		match := f.matchRow(row)
		if match && firstMatched < 0 {
			firstMatched = i
		}
		if !match && firstUnmatched < 0 {
			firstUnmatched = i
		}
		if firstMatched >= 0 && firstUnmatched >= 0 {
			break
		}
	}
	if firstMatched < 0 {
		// All rows unmatched.
		ibp.ppUnmatched.writeLogRows(workerID, lr)
		return
	}
	if firstUnmatched < 0 {
		// All rows matched.
		ibp.ppMatched.writeLogRows(workerID, lr)
		return
	}

	shard := ibp.shards.Get(workerID)
	lrMatched := &shard.lrMatched
	defer lrMatched.Reset()
	lrUnmatched := &shard.lrUnmatched
	defer lrUnmatched.Reset()

	// Split by matched and unmatched.
	for i := range lr.RowsCount() {
		row := lr.mustGetRowFields(i)
		if f.matchRow(row) {
			lrMatched.appendFromLogRows(lr, i)
		} else {
			lrUnmatched.appendFromLogRows(lr, i)
		}
	}
	ibp.ppMatched.writeLogRows(workerID, lrMatched)
	ibp.ppUnmatched.writeLogRows(workerID, lrUnmatched)
}

func (ibp *ifBranchProcessor) flush() error {
	if err := ibp.ppMatched.flush(); err != nil {
		return err
	}
	return ibp.ppUnmatched.flush()
}

func parseIfBranch(lex *lexer) (*ifBranch, error) {
	if !lex.isKeyword("if") {
		return nil, fmt.Errorf("expected keyword 'if', got %q", lex.token)
	}
	lex.nextToken()

	if !lex.isKeyword("(") {
		return nil, fmt.Errorf("condition in the 'if' statement should be wrapped with parentheses, got %s", lex.token)
	}
	lex.nextToken()

	f, err := parseFilter(lex)
	if err != nil {
		return nil, err
	}

	if !lex.isKeyword(")") {
		return nil, fmt.Errorf("condition in the 'if' statement should be wrapped with parentheses, got %s", lex.token)
	}
	lex.nextToken()

	tb, err := parseGenericTransformBlock(lex)
	if err != nil {
		return nil, err
	}

	return &ifBranch{
		f:    f,
		body: tb,
	}, nil
}

// transformSend stops processing of the current log and emits it downstream.
type transformSend struct {
}

func parseTransformSend(lex *lexer) (*transformSend, error) {
	if !lex.isKeyword("send") {
		return nil, fmt.Errorf("unexpected token: %q; want %q", lex.token, "send")
	}
	lex.nextToken()
	if !lex.isKeyword(";") {
		return nil, fmt.Errorf("expected 'send' to be ended with ';', got %q", lex.token)
	}
	lex.nextToken()
	return &transformSend{}, nil
}

func (ts *transformSend) String() string {
	return "send;"
}

func (ts *transformSend) newTransformProcessor(_ int, ppSend, _, _ pipeProcessor) pipeProcessor {
	return ppSend
}

type transformReturn struct {
}

func parseTransformReturn(lex *lexer) (*transformReturn, error) {
	if !lex.isKeyword("return") {
		return nil, fmt.Errorf("unexpected token: %q; want %q", lex.token, "return")
	}
	lex.nextToken()
	if !lex.isKeyword(";") {
		return nil, fmt.Errorf("expected 'return' to be ended with ';', got %q", lex.token)
	}
	lex.nextToken()
	return &transformReturn{}, nil
}

func (tr *transformReturn) String() string {
	return "return;"
}

func (tr *transformReturn) newTransformProcessor(_ int, _, ppReturn, _ pipeProcessor) pipeProcessor {
	return ppReturn
}

type transformDrop struct {
}

func parseTransformDrop(lex *lexer) (*transformDrop, error) {
	if !lex.isKeyword("drop") {
		return nil, fmt.Errorf("unexpected token: %q; want %q", lex.token, "drop")
	}
	lex.nextToken()
	if !lex.isKeyword(";") {
		return nil, fmt.Errorf("expected 'drop' to be ended with ';', got %q", lex.token)
	}
	lex.nextToken()
	return &transformDrop{}, nil
}

func (td *transformDrop) String() string {
	return "drop;"
}

func (td *transformDrop) newTransformProcessor(_ int, _, _, _ pipeProcessor) pipeProcessor {
	return &transformDropProcessor{}
}

type transformDropProcessor struct {
}

func (t *transformDropProcessor) writeBlock(_ uint, _ *blockResult) {
	// Drop the block.
}

func (t *transformDropProcessor) writeLogRows(_ uint, _ *LogRows) {
	// Drop lr.
}

func (t *transformDropProcessor) flush() error {
	return nil
}

// transformPipes is a single line of '|' separated pipes.
type transformPipes struct {
	pipes []pipe
}

func parseTransformPipes(lex *lexer) (*transformPipes, error) {
	pipes, err := parseTransformPipesAtLine(lex)
	if err != nil {
		return nil, err
	}
	return &transformPipes{
		pipes: pipes,
	}, nil
}

var transformsPipeParsers map[string]pipeParseFunc
var transformsPipeParsersOnce sync.Once

func getTransformsPipeParsers() map[string]pipeParseFunc {
	transformsPipeParsersOnce.Do(initTransformsPipeParsers)
	return transformsPipeParsers
}

// initTransformsPipeParsers registers the pipes allowed inside VictoriaLogs transformations.
func initTransformsPipeParsers() {
	transformsPipeParsers = map[string]pipeParseFunc{
		"coalesce":          parsePipeCoalesce,
		"collapse_nums":     parsePipeCollapseNums,
		"copy":              parsePipeCopy,
		"cp":                parsePipeCopy,
		"decolorize":        parsePipeDecolorize,
		"del":               parsePipeDelete,
		"delete":            parsePipeDelete,
		"drop_empty_fields": parsePipeDropEmptyFields,
		"extract":           parsePipeExtract,
		"extract_regexp":    parsePipeExtractRegexp,
		"eval":              parsePipeMath,
		"fields":            parsePipeFields,
		"format":            parsePipeFormat,
		"hash":              parsePipeHash,
		"json_array_len":    parsePipeJSONArrayLen,
		"keep":              parsePipeFields,
		"len":               parsePipeLen,
		"math":              parsePipeMath,
		"mv":                parsePipeRename,
		"pack_json":         parsePipePackJSON,
		"pack_logfmt":       parsePipePackLogfmt,
		"rename":            parsePipeRename,
		"replace":           parsePipeReplace,
		"replace_regexp":    parsePipeReplaceRegexp,
		"rm":                parsePipeDelete,
		"sample":            parsePipeSample,
		"set_stream_fields": parsePipeSetStreamFields,
		"split":             parsePipeSplit,
		"time_add":          parsePipeTimeAdd,
		"unpack_json":       parsePipeUnpackJSON,
		"unpack_logfmt":     parsePipeUnpackLogfmt,
		"unpack_syslog":     parsePipeUnpackSyslog,
		"unpack_words":      parsePipeUnpackWords,
		"unroll":            parsePipeUnroll,
	}
}

// parseTransformPipesAtLine parses one line of '|' separated pipes.
func parseTransformPipesAtLine(lex *lexer) ([]pipe, error) {
	var pipes []pipe
	for {
		p, err := parseTransformPipe(lex)
		if err != nil {
			return nil, err
		}
		pipes = append(pipes, p)

		switch {
		case lex.isKeyword(";"):
			// The end of the line.
			lex.nextToken()
			return pipes, nil
		case lex.isKeyword("|"):
			// Pipe delimiter, consume next pipe.
			lex.nextToken()
		default:
			return nil, fmt.Errorf("unexpected token: %q; want pipe '|' or line ';' delimiter", lex.token)
		}
	}
}

func parseTransformPipe(lex *lexer) (pipe, error) {
	pps := getTransformsPipeParsers()
	for pipeName, parseFunc := range pps {
		if !lex.isKeyword(pipeName) {
			continue
		}
		p, err := parseFunc(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %q pipe: %w", pipeName, err)
		}
		return p, nil
	}

	if isPipeKeyword(lex) {
		return nil, fmt.Errorf("pipe %q is not allowed in transformations", lex.token)
	}

	return nil, fmt.Errorf("unknown pipe with name %q", lex.token)
}

// isPipeKeyword reports whether the current token is any known pipe name.
func isPipeKeyword(lex *lexer) bool {
	pps := getPipeParsers()
	for pipeName := range pps {
		if lex.isKeyword(pipeName) {
			return true
		}
	}
	return false
}

func (tp *transformPipes) String() string {
	var s string
	for i, p := range tp.pipes {
		if i > 0 {
			s += " | "
		}
		s += p.String()
	}
	s += ";"
	return s
}

func (tp *transformPipes) newTransformProcessor(concurrency int, _, _, ppNext pipeProcessor) pipeProcessor {
	neverStopCh := make(chan struct{})
	cancel := func() {
		// The cancel function is usually required for stateful streaming pipes.
		// These types of pipes must not be used in transformations.
		panic(fmt.Errorf("BUG: 'cancel' function should not be called in transformations"))
	}
	pp := ppNext
	for i := len(tp.pipes) - 1; i >= 0; i-- {
		p := tp.pipes[i]
		pp = p.newPipeProcessor(concurrency, neverStopCh, cancel, pp)
	}
	return pp
}

// addPrefixToLines prepends prefix to every line of s.
func addPrefixToLines(s, prefix string) string {
	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = prefix + lines[i]
	}
	return strings.Join(lines, "\n")
}

// checkRecursion scans named blocks and returns an error on any recursive call.
// It does not analyze execution paths and ignores conditional guards,
// so it returns an error even if the condition is always false.
func (tp *transformsProgram) checkRecursion() error {
	namedBlockCalls := make(map[string][]string, len(tp.namedBlocks))
	for _, nb := range tp.namedBlocks {
		namedBlockCalls[nb.name] = collectNamedBlockCalls(nb.body)
	}

	visited := make(map[string]struct{})
	var stacktrace []string
	var visitFunc func(name string) error
	visitFunc = func(name string) error {
		if _, ok := visited[name]; ok {
			return nil
		}

		if slices.Contains(stacktrace, name) {
			// Recursion found, build call path and return an error.
			cycle := stacktrace
			cycle = append(cycle, name)
			for i := range cycle {
				cycle[i] = strconv.Quote(cycle[i])
			}
			return fmt.Errorf("recursive block call: %s", strings.Join(cycle, " calls "))
		}

		if _, ok := namedBlockCalls[name]; !ok {
			return fmt.Errorf("cannot find block with name %q", name)
		}

		stacktrace = append(stacktrace, name)
		for _, ref := range namedBlockCalls[name] {
			if err := visitFunc(ref); err != nil {
				return err
			}
		}
		stacktrace = stacktrace[:len(stacktrace)-1]
		visited[name] = struct{}{}

		return nil
	}

	for _, nb := range tp.namedBlocks {
		if err := visitFunc(nb.name); err != nil {
			return err
		}
	}

	return nil
}

func collectNamedBlockCalls(body *transformBlock) []string {
	var calls []string
	uniq := make(map[string]struct{})
	visitFunc := func(tr transform) bool {
		v, ok := tr.(*transformDo)
		if !ok {
			return false
		}
		if _, ok := uniq[v.blockName]; ok {
			return false
		}
		uniq[v.blockName] = struct{}{}
		calls = append(calls, v.blockName)
		return false
	}
	visitTransformRecursive(body, visitFunc)
	return calls
}

// initTransformsDo initializes transformDo with corresponding named block body.
func (tp *transformsProgram) initTransformsDo() error {
	var errGlobal error
	visitFunc := func(tr transform) bool {
		v, ok := tr.(*transformDo)
		if !ok {
			return false
		}
		n := slices.IndexFunc(tp.namedBlocks, func(nb namedTransformBlock) bool {
			return nb.name == v.blockName
		})
		if n < 0 {
			errGlobal = fmt.Errorf("cannot find block with name %q", v.blockName)
			return false
		}
		v.body = tp.namedBlocks[n].body
		return false
	}
	for _, nb := range tp.namedBlocks {
		visitTransformRecursive(nb.body, visitFunc)
	}
	visitTransformRecursive(tp.root, visitFunc)
	return errGlobal
}

// checkTimeFilter returns an error if tp has filterTime that is not allowed in log transformations.
func (tp *transformsProgram) checkTimeFilter() error {
	var errGlobal error
	visitFunc := func(f filter) bool {
		_, ok := f.(*filterTime)
		if !ok {
			return false
		}
		if errGlobal == nil {
			errGlobal = fmt.Errorf("time filters are not allowed in log transformations; got %q", f.String())
		}
		return true
	}
	visitFilterRecursiveForTransforms(tp, visitFunc)
	return errGlobal
}

// checkSubqueries returns an error if tp has subqueries that are not allowed in log transformations.
func (tp *transformsProgram) checkSubqueries() error {
	var errGlobal error
	visitFunc := func(f filter) bool {
		if !hasFilterInWithQueryForFilter(f) {
			return false
		}
		if errGlobal == nil {
			errGlobal = fmt.Errorf("subqueries are not allowed in log transformations; got %q", f.String())
		}
		return true
	}
	visitFilterRecursiveForTransforms(tp, visitFunc)
	return errGlobal
}

// visitFilterRecursiveForTransforms recursively calls visitFunc for filters inside tp.
//
// It stops calling visitFunc on the remaining transforms as soon as visitFunc returns true.
// It returns the result of the last visitFunc call.
func visitFilterRecursiveForTransforms(tp *transformsProgram, visitFunc func(f filter) bool) bool {
	visitTransformFunc := func(tr transform) bool {
		switch tr := tr.(type) {
		case *transformIf:
			for _, ib := range tr.branches {
				if visitFilterRecursive(ib.f, visitFunc) {
					return true
				}
			}
		case *transformPipes:
			for _, p := range tr.pipes {
				if visitFilterRecursiveForPipe(p, visitFunc) {
					return true
				}
			}
		}
		return false
	}
	for _, nb := range tp.namedBlocks {
		if visitTransformRecursive(nb.body, visitTransformFunc) {
			return true
		}
	}
	return visitTransformRecursive(tp.root, visitTransformFunc)
}

// visitTransformRecursive recursively calls visitFunc for transforms inside tr.
//
// It stops calling visitFunc on the remaining transforms as soon as visitFunc returns true.
// It returns the result of the last visitFunc call.
func visitTransformRecursive(tr transform, visitFunc func(tr transform) bool) bool {
	// Visit the tr itself.
	if visitFunc(tr) {
		return true
	}

	// Visit nested transforms.
	switch tr := tr.(type) {
	case *transformBlock:
		for _, subTr := range tr.transforms {
			if visitTransformRecursive(subTr, visitFunc) {
				return true
			}
		}
	case *transformIf:
		for _, ib := range tr.branches {
			if visitTransformRecursive(ib.body, visitFunc) {
				return true
			}
		}
		if tr.elseBody != nil {
			if visitTransformRecursive(tr.elseBody, visitFunc) {
				return true
			}
		}
	}
	return false
}

// visitFilterRecursiveForPipe recursively calls visitFunc for filters inside p.
//
// It stops calling visitFunc on the remaining filters as soon as visitFunc returns true.
// It returns the result of the last visitFunc call.
func visitFilterRecursiveForPipe(p pipe, visitFunc func(f filter) bool) bool {
	// All possible pipes that may contain a nested filter.
	switch p := p.(type) {
	case *pipeCollapseNums:
		if p.iff != nil {
			return visitFilterRecursive(p.iff.f, visitFunc)
		}
	case *pipeExtract:
		if p.iff != nil {
			return visitFilterRecursive(p.iff.f, visitFunc)
		}
	case *pipeExtractRegexp:
		if p.iff != nil {
			return visitFilterRecursive(p.iff.f, visitFunc)
		}
	case *pipeFormat:
		if p.iff != nil {
			return visitFilterRecursive(p.iff.f, visitFunc)
		}
	case *pipeReplace:
		if p.iff != nil {
			return visitFilterRecursive(p.iff.f, visitFunc)
		}
	case *pipeReplaceRegexp:
		if p.iff != nil {
			return visitFilterRecursive(p.iff.f, visitFunc)
		}
	case *pipeSetStreamFields:
		if p.iff != nil {
			return visitFilterRecursive(p.iff.f, visitFunc)
		}
	case *pipeUnpackJSON:
		if p.iff != nil {
			return visitFilterRecursive(p.iff.f, visitFunc)
		}
	case *pipeUnpackLogfmt:
		if p.iff != nil {
			return visitFilterRecursive(p.iff.f, visitFunc)
		}
	case *pipeUnpackSyslog:
		if p.iff != nil {
			return visitFilterRecursive(p.iff.f, visitFunc)
		}
	case *pipeUnroll:
		if p.iff != nil {
			return visitFilterRecursive(p.iff.f, visitFunc)
		}
	case *pipeFilter:
		if p.f != nil {
			return visitFilterRecursive(p.f, visitFunc)
		}
	}
	// Does not have filter.
	return false
}
