package drain

import (
	"reflect"
	"testing"
)

func TestDrain(t *testing.T) {
	config := DefaultConfig()
	d := New(config, nil)

	// Test training
	d.Train("user 123 login")
	d.Train("user 456 login")
	d.Train("other message")

	clusters := d.Clusters()
	if len(clusters) != 2 {
		t.Fatalf("expected 2 clusters, got %d", len(clusters))
	}

	// Test template extraction
	expectedTemplates := []string{
		"user <*> login",
		"other message",
	}
	templates := []string{clusters[0].Template(), clusters[1].Template()}
	if !reflect.DeepEqual(templates, expectedTemplates) {
		t.Errorf("expected templates %v, got %v", expectedTemplates, templates)
	}

	// Test TrainWithHits
	d2 := New(config, nil)
	d2.TrainWithHits("user 1 login", 10)
	d2.TrainWithHits("user 2 login", 20)

	clusters2 := d2.Clusters()
	if len(clusters2) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(clusters2))
	}
	if clusters2[0].Size() != 30 {
		t.Errorf("expected size 30, got %d", clusters2[0].Size())
	}

	// Test Merge
	d.Merge(d2)
	// "user <*> login" should now have hits from both
	match := d.Match("user 999 login")
	if match == nil {
		t.Fatal("expected match for 'user 999 login'")
	}
	if match.Size() != 32 { // 2 from first d, 30 from d2
		t.Errorf("expected size 32 after merge, got %d", match.Size())
	}
}

func TestDrainMemoryBudget(t *testing.T) {
	config := DefaultConfig()
	budget := 1000
	d := New(config, &budget)

	initialBudget := budget
	d.Train("some very long log message to consume some memory budget")

	if budget >= initialBudget {
		t.Errorf("expected budget to decrease, got %d (initial %d)", budget, initialBudget)
	}
}

func TestDrainMerge(t *testing.T) {
	config := DefaultConfig()

	// Scenario 1: Merging identical clusters
	d1 := New(config, nil)
	d1.TrainWithHits("user 123 login", 10)
	d2 := New(config, nil)
	d2.TrainWithHits("user 456 login", 20)

	d1.Merge(d2)
	clusters := d1.Clusters()
	if len(clusters) != 1 {
		t.Fatalf("expected 1 cluster for identical templates, got %d", len(clusters))
	}
	if clusters[0].Template() != "user <*> login" {
		t.Errorf("expected template 'user <*> login', got '%s'", clusters[0].Template())
	}
	if clusters[0].Size() != 30 {
		t.Errorf("expected size 30, got %d", clusters[0].Size())
	}

	// Scenario 2: Merging different clusters
	d3 := New(config, nil)
	d3.TrainWithHits("system startup", 5)
	d1.Merge(d3)
	clusters = d1.Clusters()
	if len(clusters) != 2 {
		t.Fatalf("expected 2 clusters after merging different templates, got %d", len(clusters))
	}

	// Scenario 3: Template generalization during merge
	// d1 has "user <*> login" and "system startup"
	d4 := New(config, nil)
	d4.TrainWithHits("system shutdown", 10)
	d1.Merge(d4)
	// "system startup" + "system shutdown" -> "system <*>"
	match := d1.Match("system reboots")
	if match == nil {
		t.Fatal("expected match for 'system reboots' after generalization")
	}
	if match.Template() != "system <*>" {
		t.Errorf("expected generalized template 'system <*>', got '%s'", match.Template())
	}
	if match.Size() != 15 {
		t.Errorf("expected size 15 for generalized cluster, got %d", match.Size())
	}

	// Scenario 4: Empty merge
	dEmpty := New(config, nil)
	dBefore := New(config, nil)
	dBefore.Train("msg")
	dBefore.Merge(dEmpty)
	if len(dBefore.Clusters()) != 1 {
		t.Errorf("expected 1 cluster after empty merge, got %d", len(dBefore.Clusters()))
	}

	dEmpty.Merge(dBefore)
	if len(dEmpty.Clusters()) != 1 {
		t.Errorf("expected 1 cluster in empty instance after merge, got %d", len(dEmpty.Clusters()))
	}
}
