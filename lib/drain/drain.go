package drain

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
	"unsafe"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

type Config struct {
	maxNodeDepth    int
	LogClusterDepth int
	SimTh           float64
	MaxChildren     int
	ExtraDelimiters []string
	MaxClusters     int
	ParamString     string
}

type LogCluster struct {
	logTemplateTokens []string
	id                int
	size              uint64
}

func (c *LogCluster) Size() uint64 {
	return c.size
}

func (c *LogCluster) ID() int {
	return c.id
}

func (c *LogCluster) Template() string {
	return strings.Join(c.logTemplateTokens, " ")
}
func (c *LogCluster) String() string {
	return fmt.Sprintf("id={%d} : size={%d} : %s", c.id, c.size, c.Template())
}

func createLogClusterCache(maxSize int) *LogClusterCache {
	if maxSize == 0 {
		maxSize = math.MaxInt
	}
	cache, _ := simplelru.NewLRU[int, *LogCluster](maxSize, nil)
	return &LogClusterCache{
		cache: cache,
	}
}

type LogClusterCache struct {
	cache simplelru.LRUCache[int, *LogCluster]
}

func (c *LogClusterCache) Values() []*LogCluster {
	values := make([]*LogCluster, 0)
	for _, key := range c.cache.Keys() {
		if value, ok := c.cache.Peek(key); ok {
			values = append(values, value)
		}
	}
	return values
}

func (c *LogClusterCache) Set(key int, cluster *LogCluster) {
	c.cache.Add(key, cluster)
}

func (c *LogClusterCache) Get(key int) *LogCluster {
	cluster, ok := c.cache.Get(key)
	if !ok {
		return nil
	}
	return cluster
}

func createNode(stateSizeBudget *int) *Node {
	if stateSizeBudget != nil {
		*stateSizeBudget -= int(nodeSize)
	}
	return &Node{
		keyToChildNode: make(map[string]*Node),
		clusterIDs:     make([]int, 0),
	}
}

const nodeSize = unsafe.Sizeof(Node{}) + 24 // 24 for map overhead estimation
const clusterSize = unsafe.Sizeof(LogCluster{})

type Node struct {
	keyToChildNode map[string]*Node
	clusterIDs     []int
}

func DefaultConfig() *Config {
	return &Config{
		LogClusterDepth: 4,
		SimTh:           0.4,
		MaxChildren:     100,
		ParamString:     "<*>",
		MaxClusters:     1000,
	}
}

func New(config *Config, stateSizeBudget *int) *Drain {
	if config.LogClusterDepth < 3 {
		panic("depth argument must be at least 3")
	}
	config.maxNodeDepth = config.LogClusterDepth - 2

	d := &Drain{
		config:          config,
		rootNode:        createNode(stateSizeBudget),
		idToCluster:     createLogClusterCache(config.MaxClusters),
		stateSizeBudget: stateSizeBudget,
	}
	return d
}

type Drain struct {
	config          *Config
	rootNode        *Node
	idToCluster     *LogClusterCache
	clustersCounter int

	stateSizeBudget *int
}

func (d *Drain) Clusters() []*LogCluster {
	return d.idToCluster.Values()
}

func (d *Drain) Merge(other *Drain) {
	for _, cluster := range other.Clusters() {
		d.TrainWithHits(cluster.Template(), cluster.Size())
	}
}

func (d *Drain) Train(content string) *LogCluster {
	return d.TrainWithHits(content, 1)
}

func (d *Drain) TrainWithHits(content string, hits uint64) *LogCluster {
	contentTokens := d.getContentAsTokens(content)

	matchCluster := d.treeSearch(d.rootNode, contentTokens, d.config.SimTh, false)
	// Match no existing log cluster
	if matchCluster == nil {
		d.clustersCounter++
		clusterID := d.clustersCounter
		matchCluster = &LogCluster{
			logTemplateTokens: d.cloneTokens(contentTokens),
			id:                clusterID,
			size:              hits,
		}
		if d.stateSizeBudget != nil {
			*d.stateSizeBudget -= int(clusterSize)
		}
		d.idToCluster.Set(clusterID, matchCluster)
		d.addSeqToPrefixTree(d.rootNode, matchCluster)
	} else {
		newTemplateTokens := d.createTemplate(contentTokens, matchCluster.logTemplateTokens)
		matchCluster.logTemplateTokens = newTemplateTokens
		matchCluster.size += hits
		// Touch cluster to update its state in the cache.
		d.idToCluster.Get(matchCluster.id)
	}
	return matchCluster
}

func (d *Drain) cloneTokens(tokens []string) []string {
	dst := make([]string, len(tokens))
	size := len(tokens) * int(unsafe.Sizeof(""))
	for i, token := range tokens {
		s := strings.Clone(token)
		dst[i] = s
		size += len(s)
	}
	if d.stateSizeBudget != nil {
		*d.stateSizeBudget -= size
	}
	return dst
}

// Match against an already existing cluster. Match shall be perfect (sim_th=1.0). New cluster will not be created as a result of this call, nor any cluster modifications.
func (d *Drain) Match(content string) *LogCluster {
	contentTokens := d.getContentAsTokens(content)
	matchCluster := d.treeSearch(d.rootNode, contentTokens, 1.0, true)
	return matchCluster
}

func (d *Drain) getContentAsTokens(content string) []string {
	content = strings.TrimSpace(content)
	for _, extraDelimiter := range d.config.ExtraDelimiters {
		content = strings.Replace(content, extraDelimiter, " ", -1)
	}
	return strings.Split(content, " ")
}

func (d *Drain) treeSearch(rootNode *Node, tokens []string, simTh float64, includeParams bool) *LogCluster {
	tokenCount := len(tokens)

	// at first level, children are grouped by token (word) count
	curNode, ok := rootNode.keyToChildNode[strconv.Itoa(tokenCount)]

	// no template with same token count yet
	if !ok {
		return nil
	}

	// handle case of empty log string - return the single cluster in that group
	if tokenCount == 0 {
		return d.idToCluster.Get(curNode.clusterIDs[0])
	}

	// find the leaf node for this log - a path of nodes matching the first N tokens (N=tree depth)
	curNodeDepth := 1
	for _, token := range tokens {
		// at max depth
		if curNodeDepth >= d.config.maxNodeDepth {
			break
		}

		// this is last token
		if curNodeDepth == tokenCount {
			break
		}

		keyToChildNode := curNode.keyToChildNode
		curNode, ok = keyToChildNode[token]
		if !ok { // no exact next token exist, try wildcard node
			curNode, ok = keyToChildNode[d.config.ParamString]
		}
		if !ok { // no wildcard node exist
			return nil
		}
		curNodeDepth++
	}

	// get best match among all clusters with same prefix, or None if no match is above sim_th
	cluster := d.fastMatch(curNode.clusterIDs, tokens, simTh, includeParams)
	return cluster
}

// fastMatch Find the best match for a log message (represented as tokens) versus a list of clusters
func (d *Drain) fastMatch(clusterIDs []int, tokens []string, simTh float64, includeParams bool) *LogCluster {
	var matchCluster, maxCluster *LogCluster

	maxSim := -1.0
	maxParamCount := -1
	for _, clusterID := range clusterIDs {
		// Try to retrieve cluster from cache with bypassing eviction
		// algorithm as we are only testing candidates for a match.
		cluster := d.idToCluster.Get(clusterID)
		if cluster == nil {
			continue
		}
		curSim, paramCount := d.getSeqDistance(cluster.logTemplateTokens, tokens, includeParams)
		if curSim > maxSim || (curSim == maxSim && paramCount > maxParamCount) {
			maxSim = curSim
			maxParamCount = paramCount
			maxCluster = cluster
		}
	}
	if maxSim >= simTh {
		matchCluster = maxCluster
	}
	return matchCluster
}

func (d *Drain) getSeqDistance(seq1, seq2 []string, includeParams bool) (float64, int) {
	if len(seq1) != len(seq2) {
		panic("seq1 seq2 be of same length")
	}

	simTokens := 0
	paramCount := 0
	for i := range seq1 {
		token1 := seq1[i]
		token2 := seq2[i]
		if token1 == d.config.ParamString {
			paramCount++
		} else if token1 == token2 {
			simTokens++
		}
	}
	if includeParams {
		simTokens += paramCount
	}
	retVal := float64(simTokens) / float64(len(seq1))
	return retVal, paramCount
}

func (d *Drain) addSeqToPrefixTree(rootNode *Node, cluster *LogCluster) {
	tokenCount := len(cluster.logTemplateTokens)
	tokenCountStr := strconv.Itoa(tokenCount)

	firstLayerNode, ok := rootNode.keyToChildNode[tokenCountStr]
	if !ok {
		firstLayerNode = createNode(d.stateSizeBudget)
		rootNode.keyToChildNode[tokenCountStr] = firstLayerNode
	}
	curNode := firstLayerNode

	// handle case of empty log string
	if tokenCount == 0 {
		curNode.clusterIDs = append(curNode.clusterIDs, cluster.id)
		return
	}

	currentDepth := 1
	for _, token := range cluster.logTemplateTokens {
		// if at max depth or this is last token in template - add current log cluster to the leaf node
		if (currentDepth >= d.config.maxNodeDepth) || currentDepth >= tokenCount {
			// clean up stale clusters before adding a new one.
			newClusterIDs := make([]int, 0, len(curNode.clusterIDs))
			for _, clusterID := range curNode.clusterIDs {
				if d.idToCluster.Get(clusterID) != nil {
					newClusterIDs = append(newClusterIDs, clusterID)
				}
			}
			newClusterIDs = append(newClusterIDs, cluster.id)
			curNode.clusterIDs = newClusterIDs
			break
		}

		// if token not matched in this layer of existing tree.
		if _, ok = curNode.keyToChildNode[token]; !ok {
			// if token not matched in this layer of existing tree.
			if !d.hasNumbers(token) {
				if _, ok = curNode.keyToChildNode[d.config.ParamString]; ok {
					if len(curNode.keyToChildNode) < d.config.MaxChildren {
						newNode := createNode(d.stateSizeBudget)
						s := strings.Clone(token)
						if d.stateSizeBudget != nil {
							*d.stateSizeBudget -= len(s) + 16 // 16 for map entry overhead
						}
						curNode.keyToChildNode[s] = newNode
						curNode = newNode
					} else {
						curNode = curNode.keyToChildNode[d.config.ParamString]
					}
				} else {
					if len(curNode.keyToChildNode)+1 < d.config.MaxChildren {
						newNode := createNode(d.stateSizeBudget)
						s := strings.Clone(token)
						if d.stateSizeBudget != nil {
							*d.stateSizeBudget -= len(s) + 16
						}
						curNode.keyToChildNode[s] = newNode
						curNode = newNode
					} else if len(curNode.keyToChildNode)+1 == d.config.MaxChildren {
						newNode := createNode(d.stateSizeBudget)
						curNode.keyToChildNode[d.config.ParamString] = newNode
						curNode = newNode
					} else {
						curNode = curNode.keyToChildNode[d.config.ParamString]
					}
				}
			} else {
				if _, ok = curNode.keyToChildNode[d.config.ParamString]; !ok {
					newNode := createNode(d.stateSizeBudget)
					curNode.keyToChildNode[d.config.ParamString] = newNode
					curNode = newNode
				} else {
					curNode = curNode.keyToChildNode[d.config.ParamString]
				}
			}
		} else {
			// if the token is matched
			curNode = curNode.keyToChildNode[token]
		}

		currentDepth++
	}
}

func (d *Drain) hasNumbers(s string) bool {
	for _, c := range s {
		if unicode.IsNumber(c) {
			return true
		}
	}
	return false
}

func (d *Drain) createTemplate(seq1, seq2 []string) []string {
	if len(seq1) != len(seq2) {
		panic("seq1 seq2 be of same length")
	}
	retVal := make([]string, len(seq2))
	copy(retVal, seq2)
	for i := range seq1 {
		if seq1[i] != seq2[i] {
			retVal[i] = d.config.ParamString
		}
	}
	return retVal
}
