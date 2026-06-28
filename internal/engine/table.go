package engine

// RuleTable holds the compiled rule set, sorted by priority ascending.
// Replaced atomically on hot-reload. Never mutated in place.
type RuleTable struct {
	rules   []*CompiledRule
	version int64
}

// NewRuleTable creates a new rule table.
func NewRuleTable(rules []*CompiledRule, version int64) *RuleTable {
	return &RuleTable{rules: rules, version: version}
}

// Version returns the table version.
func (t *RuleTable) Version() int64 { return t.version }

// Rules returns the compiled rules.
func (t *RuleTable) Rules() []*CompiledRule { return t.rules }
