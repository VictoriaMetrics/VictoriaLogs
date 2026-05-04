package filecollector

import "testing"

// TestMatchesAnyExcludeGlob_AllPatternsApply pins the fix for
// https://github.com/VictoriaMetrics/VictoriaLogs/issues/1374. Previously
// the matcher only consulted excludeGlob[argIdx], where argIdx was the
// index of the include-glob that matched the file — so any excludeGlob
// beyond the include-glob count was silently dropped. The new helper
// consults every configured excludeGlob, regardless of arg index.
func TestMatchesAnyExcludeGlob_AllPatternsApply(t *testing.T) {
	cases := []struct {
		name            string
		filePath        string
		excludePatterns []string
		expectExcluded  bool
	}{
		{
			name:            "no patterns means no exclusion",
			filePath:        "/var/log/app/audit.log",
			excludePatterns: nil,
			expectExcluded:  false,
		},
		{
			name:            "single pattern matches",
			filePath:        "/var/log/app/debug.log",
			excludePatterns: []string{"/var/log/app/debug*.log"},
			expectExcluded:  true,
		},
		{
			name:            "single pattern no match",
			filePath:        "/var/log/app/info.log",
			excludePatterns: []string{"/var/log/app/debug*.log"},
			expectExcluded:  false,
		},
		{
			// Direct repro of issue #1374: two exclude patterns, file
			// matches the *second* one. Old `GetOptionalArg(argIdx)` path
			// would only check the first pattern (or the one at the same
			// arg-index as the include-glob) and miss this.
			name:            "second pattern catches what first does not (issue #1374)",
			filePath:        "/var/log/app/audit.log",
			excludePatterns: []string{"/var/log/app/debug*.log", "/var/log/app/audit*.log"},
			expectExcluded:  true,
		},
		{
			name:            "third pattern still applied",
			filePath:        "/srv/logs/secret.log",
			excludePatterns: []string{"/srv/logs/debug*.log", "/srv/logs/info*.log", "/srv/logs/secret*.log"},
			expectExcluded:  true,
		},
		{
			name:            "empty patterns are tolerated and ignored",
			filePath:        "/var/log/app/info.log",
			excludePatterns: []string{"", "/var/log/app/debug*.log", ""},
			expectExcluded:  false,
		},
		{
			// A bare empty pattern should not exclude every file. Without
			// the explicit empty-pattern guard, filepath.Match("", path)
			// returns no match, so this is mostly defensive — but pinning
			// the behaviour now keeps a future change from regressing it.
			name:            "lone empty pattern does not exclude anything",
			filePath:        "/var/log/app/info.log",
			excludePatterns: []string{""},
			expectExcluded:  false,
		},
		{
			name:            "wildcard star matches everything",
			filePath:        "/var/log/app/info.log",
			excludePatterns: []string{"/var/log/*/info.log"},
			expectExcluded:  true,
		},
		{
			// filepath.Match treats `?` as a single-char wildcard. The
			// helper's behaviour should pass through unchanged.
			name:            "single-char wildcard pattern",
			filePath:        "/var/log/a.log",
			excludePatterns: []string{"/var/log/?.log"},
			expectExcluded:  true,
		},
		{
			name:            "non-matching pattern in front, matching one behind",
			filePath:        "/etc/secrets.log",
			excludePatterns: []string{"/var/log/*.log", "/etc/secrets*.log"},
			expectExcluded:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := matchesAnyExcludeGlob(tc.filePath, tc.excludePatterns)
			if got != tc.expectExcluded {
				t.Errorf("matchesAnyExcludeGlob(%q, %v): got %v, want %v",
					tc.filePath, tc.excludePatterns, got, tc.expectExcluded)
			}
		})
	}
}
