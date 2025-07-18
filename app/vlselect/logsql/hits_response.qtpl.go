// Code generated by qtc from "hits_response.qtpl". DO NOT EDIT.
// See https://github.com/valyala/quicktemplate for details.

//line app/vlselect/logsql/hits_response.qtpl:1
package logsql

//line app/vlselect/logsql/hits_response.qtpl:1
import (
	"slices"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// FieldsForHits formats labels for /select/logsql/hits response

//line app/vlselect/logsql/hits_response.qtpl:10
import (
	qtio422016 "io"

	qt422016 "github.com/valyala/quicktemplate"
)

//line app/vlselect/logsql/hits_response.qtpl:10
var (
	_ = qtio422016.Copy
	_ = qt422016.AcquireByteBuffer
)

//line app/vlselect/logsql/hits_response.qtpl:10
func StreamFieldsForHits(qw422016 *qt422016.Writer, columns []logstorage.BlockColumn, rowIdx int) {
//line app/vlselect/logsql/hits_response.qtpl:10
	qw422016.N().S(`{`)
//line app/vlselect/logsql/hits_response.qtpl:12
	if len(columns) > 0 {
//line app/vlselect/logsql/hits_response.qtpl:13
		qw422016.N().Q(columns[0].Name)
//line app/vlselect/logsql/hits_response.qtpl:13
		qw422016.N().S(`:`)
//line app/vlselect/logsql/hits_response.qtpl:13
		qw422016.N().Q(columns[0].Values[rowIdx])
//line app/vlselect/logsql/hits_response.qtpl:14
		for _, c := range columns[1:] {
//line app/vlselect/logsql/hits_response.qtpl:14
			qw422016.N().S(`,`)
//line app/vlselect/logsql/hits_response.qtpl:15
			qw422016.N().Q(c.Name)
//line app/vlselect/logsql/hits_response.qtpl:15
			qw422016.N().S(`:`)
//line app/vlselect/logsql/hits_response.qtpl:15
			qw422016.N().Q(c.Values[rowIdx])
//line app/vlselect/logsql/hits_response.qtpl:16
		}
//line app/vlselect/logsql/hits_response.qtpl:17
	}
//line app/vlselect/logsql/hits_response.qtpl:17
	qw422016.N().S(`}`)
//line app/vlselect/logsql/hits_response.qtpl:19
}

//line app/vlselect/logsql/hits_response.qtpl:19
func WriteFieldsForHits(qq422016 qtio422016.Writer, columns []logstorage.BlockColumn, rowIdx int) {
//line app/vlselect/logsql/hits_response.qtpl:19
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vlselect/logsql/hits_response.qtpl:19
	StreamFieldsForHits(qw422016, columns, rowIdx)
//line app/vlselect/logsql/hits_response.qtpl:19
	qt422016.ReleaseWriter(qw422016)
//line app/vlselect/logsql/hits_response.qtpl:19
}

//line app/vlselect/logsql/hits_response.qtpl:19
func FieldsForHits(columns []logstorage.BlockColumn, rowIdx int) string {
//line app/vlselect/logsql/hits_response.qtpl:19
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vlselect/logsql/hits_response.qtpl:19
	WriteFieldsForHits(qb422016, columns, rowIdx)
//line app/vlselect/logsql/hits_response.qtpl:19
	qs422016 := string(qb422016.B)
//line app/vlselect/logsql/hits_response.qtpl:19
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vlselect/logsql/hits_response.qtpl:19
	return qs422016
//line app/vlselect/logsql/hits_response.qtpl:19
}

//line app/vlselect/logsql/hits_response.qtpl:21
func StreamHitsSeries(qw422016 *qt422016.Writer, m map[string]*hitsSeries) {
//line app/vlselect/logsql/hits_response.qtpl:21
	qw422016.N().S(`{`)
//line app/vlselect/logsql/hits_response.qtpl:24
	sortedKeys := make([]string, 0, len(m))
	for k := range m {
		sortedKeys = append(sortedKeys, k)
	}
	slices.Sort(sortedKeys)

//line app/vlselect/logsql/hits_response.qtpl:29
	qw422016.N().S(`"hits":[`)
//line app/vlselect/logsql/hits_response.qtpl:31
	if len(sortedKeys) > 0 {
//line app/vlselect/logsql/hits_response.qtpl:32
		streamhitsSeriesLine(qw422016, m, sortedKeys[0])
//line app/vlselect/logsql/hits_response.qtpl:33
		for _, k := range sortedKeys[1:] {
//line app/vlselect/logsql/hits_response.qtpl:33
			qw422016.N().S(`,`)
//line app/vlselect/logsql/hits_response.qtpl:34
			streamhitsSeriesLine(qw422016, m, k)
//line app/vlselect/logsql/hits_response.qtpl:35
		}
//line app/vlselect/logsql/hits_response.qtpl:36
	}
//line app/vlselect/logsql/hits_response.qtpl:36
	qw422016.N().S(`]}`)
//line app/vlselect/logsql/hits_response.qtpl:39
}

//line app/vlselect/logsql/hits_response.qtpl:39
func WriteHitsSeries(qq422016 qtio422016.Writer, m map[string]*hitsSeries) {
//line app/vlselect/logsql/hits_response.qtpl:39
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vlselect/logsql/hits_response.qtpl:39
	StreamHitsSeries(qw422016, m)
//line app/vlselect/logsql/hits_response.qtpl:39
	qt422016.ReleaseWriter(qw422016)
//line app/vlselect/logsql/hits_response.qtpl:39
}

//line app/vlselect/logsql/hits_response.qtpl:39
func HitsSeries(m map[string]*hitsSeries) string {
//line app/vlselect/logsql/hits_response.qtpl:39
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vlselect/logsql/hits_response.qtpl:39
	WriteHitsSeries(qb422016, m)
//line app/vlselect/logsql/hits_response.qtpl:39
	qs422016 := string(qb422016.B)
//line app/vlselect/logsql/hits_response.qtpl:39
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vlselect/logsql/hits_response.qtpl:39
	return qs422016
//line app/vlselect/logsql/hits_response.qtpl:39
}

//line app/vlselect/logsql/hits_response.qtpl:41
func streamhitsSeriesLine(qw422016 *qt422016.Writer, m map[string]*hitsSeries, k string) {
//line app/vlselect/logsql/hits_response.qtpl:41
	qw422016.N().S(`{`)
//line app/vlselect/logsql/hits_response.qtpl:44
	hs := m[k]
	hs.sort()
	timestamps := hs.timestamps
	hits := hs.hits

//line app/vlselect/logsql/hits_response.qtpl:48
	qw422016.N().S(`"fields":`)
//line app/vlselect/logsql/hits_response.qtpl:49
	qw422016.N().S(k)
//line app/vlselect/logsql/hits_response.qtpl:49
	qw422016.N().S(`,"timestamps":[`)
//line app/vlselect/logsql/hits_response.qtpl:51
	if len(timestamps) > 0 {
//line app/vlselect/logsql/hits_response.qtpl:52
		qw422016.N().Q(timestamps[0])
//line app/vlselect/logsql/hits_response.qtpl:53
		for _, ts := range timestamps[1:] {
//line app/vlselect/logsql/hits_response.qtpl:53
			qw422016.N().S(`,`)
//line app/vlselect/logsql/hits_response.qtpl:54
			qw422016.N().Q(ts)
//line app/vlselect/logsql/hits_response.qtpl:55
		}
//line app/vlselect/logsql/hits_response.qtpl:56
	}
//line app/vlselect/logsql/hits_response.qtpl:56
	qw422016.N().S(`],"values":[`)
//line app/vlselect/logsql/hits_response.qtpl:59
	if len(hits) > 0 {
//line app/vlselect/logsql/hits_response.qtpl:60
		qw422016.N().DUL(hits[0])
//line app/vlselect/logsql/hits_response.qtpl:61
		for _, v := range hits[1:] {
//line app/vlselect/logsql/hits_response.qtpl:61
			qw422016.N().S(`,`)
//line app/vlselect/logsql/hits_response.qtpl:62
			qw422016.N().DUL(v)
//line app/vlselect/logsql/hits_response.qtpl:63
		}
//line app/vlselect/logsql/hits_response.qtpl:64
	}
//line app/vlselect/logsql/hits_response.qtpl:64
	qw422016.N().S(`],"total":`)
//line app/vlselect/logsql/hits_response.qtpl:66
	qw422016.N().DUL(hs.hitsTotal)
//line app/vlselect/logsql/hits_response.qtpl:66
	qw422016.N().S(`}`)
//line app/vlselect/logsql/hits_response.qtpl:68
}

//line app/vlselect/logsql/hits_response.qtpl:68
func writehitsSeriesLine(qq422016 qtio422016.Writer, m map[string]*hitsSeries, k string) {
//line app/vlselect/logsql/hits_response.qtpl:68
	qw422016 := qt422016.AcquireWriter(qq422016)
//line app/vlselect/logsql/hits_response.qtpl:68
	streamhitsSeriesLine(qw422016, m, k)
//line app/vlselect/logsql/hits_response.qtpl:68
	qt422016.ReleaseWriter(qw422016)
//line app/vlselect/logsql/hits_response.qtpl:68
}

//line app/vlselect/logsql/hits_response.qtpl:68
func hitsSeriesLine(m map[string]*hitsSeries, k string) string {
//line app/vlselect/logsql/hits_response.qtpl:68
	qb422016 := qt422016.AcquireByteBuffer()
//line app/vlselect/logsql/hits_response.qtpl:68
	writehitsSeriesLine(qb422016, m, k)
//line app/vlselect/logsql/hits_response.qtpl:68
	qs422016 := string(qb422016.B)
//line app/vlselect/logsql/hits_response.qtpl:68
	qt422016.ReleaseByteBuffer(qb422016)
//line app/vlselect/logsql/hits_response.qtpl:68
	return qs422016
//line app/vlselect/logsql/hits_response.qtpl:68
}
