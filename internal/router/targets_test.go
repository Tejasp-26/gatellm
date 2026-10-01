package router

import (
	"strings"
	"testing"

	"gatellm/internal/provider"
)

func testRegistry() provider.Registry {
	return provider.Registry{
		"mock":   provider.NewMock(0, 0),
		"mock-b": provider.NewMockNamed("mock-b", 0, 0),
		"groq":   provider.NewGroq("k"),
		"gemini": provider.NewGemini("k"),
	}
}

func TestParseTargets(t *testing.T) {
	ts, err := ParseTargets("groq/llama-3.1-8b-instant:3, gemini/gemini-2.5-flash ,mock", testRegistry())
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 3 {
		t.Fatalf("got %d targets", len(ts))
	}
	if ts[0].Name() != "groq" || ts[0].Model != "llama-3.1-8b-instant" || ts[0].Weight != 3 {
		t.Errorf("first target wrong: %+v", ts[0])
	}
	if ts[1].Name() != "gemini" || ts[1].Model != "gemini-2.5-flash" || ts[1].Weight != 1 {
		t.Errorf("second target wrong (weight defaults to 1): %+v", ts[1])
	}
	if ts[2].Name() != "mock" || ts[2].Model != "" {
		t.Errorf("third target wrong: %+v", ts[2])
	}
}

func TestParseTargetsAcceptsTheSecondMock(t *testing.T) {
	ts, err := ParseTargets("mock,mock-b:2", testRegistry())
	if err != nil || ts[1].Name() != "mock-b" || ts[1].Weight != 2 {
		t.Errorf("mock-b should work without a model: %v %+v", err, ts)
	}
}

func TestParseTargetsErrors(t *testing.T) {
	reg := provider.Registry{"mock": provider.NewMock(0, 0), "groq": provider.NewGroq("k")}
	tests := []struct {
		name, spec, wantInMessage string
	}{
		{"empty", "", "no targets"},
		{"only commas", " , ,", "no targets"},
		{"provider not enabled", "gemini/x", "not enabled"},
		{"unknown provider", "foo/bar", "not enabled"},
		{"missing model", "groq", "groq/<model-name>"},
		{"weight zero", "groq/x:0", "weight"},
		{"weight not a number", "groq/x:many", "weight"},
		{"negative weight", "groq/x:-2", "weight"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTargets(tc.spec, reg)
			if err == nil || !strings.Contains(err.Error(), tc.wantInMessage) {
				t.Errorf("error = %v, want it to mention %q", err, tc.wantInMessage)
			}
		})
	}
}

func TestDefaultTargetSpec(t *testing.T) {
	only := func(names ...string) provider.Registry {
		r := provider.Registry{"mock": provider.NewMock(0, 0)}
		for _, n := range names {
			r[n] = provider.NewMock(0, 0)
		}
		return r
	}
	if got := DefaultTargetSpec(only()); got != "mock" {
		t.Errorf("no real providers -> %q, want mock", got)
	}
	if got := DefaultTargetSpec(only("groq")); got != "groq/llama-3.1-8b-instant" {
		t.Errorf("groq only -> %q", got)
	}
	got := DefaultTargetSpec(only("groq", "gemini"))
	if !strings.HasPrefix(got, "groq/") || !strings.Contains(got, ",gemini/") {
		t.Errorf("both -> %q", got)
	}
	if strings.Contains(got, "mock") {
		t.Error("the mock must not be a default fallback when real providers exist: users would get fake answers")
	}
}
