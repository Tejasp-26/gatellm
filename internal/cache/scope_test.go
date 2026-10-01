package cache

import (
	"strings"
	"testing"

	"gatellm/internal/provider"
)

func TestScopeSeparatesWhatChangesTheAnswer(t *testing.T) {
	base := Scope("mock", baseReq())
	if Scope("mock", baseReq()) != base {
		t.Error("same input, same scope")
	}
	r := baseReq()
	r.MaxTokens = 51
	if Scope("mock", r) == base {
		t.Error("max_tokens must change the scope")
	}
	r = baseReq()
	r.Temperature = f64(0.5)
	if Scope("mock", r) == base {
		t.Error("temperature must change the scope")
	}
	r.Temperature = nil
	if Scope("mock", r) == base {
		t.Error("no temperature is not the same as 0")
	}
	if Scope("auto", baseReq()) == base {
		t.Error("the model name must change the scope")
	}
}

func TestPromptTextKeepsRolesAndOrder(t *testing.T) {
	text, ok := PromptText(baseReq())
	if !ok || text != "system: be short\nuser: hello\n" {
		t.Errorf("got %q ok=%v", text, ok)
	}
	r := baseReq()
	r.Messages[0].Role = "user"
	other, _ := PromptText(r)
	if other == text {
		t.Error("the role must be part of the text")
	}
}

func TestPromptTextTooLongIsRefused(t *testing.T) {
	r := &provider.ChatRequest{Messages: []provider.Message{{Role: "user", Content: strings.Repeat("a", MaxSemanticChars)}}}
	if _, ok := PromptText(r); ok {
		t.Error("a prompt over the limit must not use the semantic cache")
	}
	r.Messages[0].Content = "short"
	if _, ok := PromptText(r); !ok {
		t.Error("a short prompt is fine")
	}
}
