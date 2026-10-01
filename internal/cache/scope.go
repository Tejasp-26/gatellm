package cache

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"gatellm/internal/provider"
)

// MaxSemanticChars: a longer prompt is not used for the semantic cache.
// The embedding model reads a limited amount of text, and cutting a long prompt would make
// two different prompts look the same. Long prompts still use the exact cache.
const MaxSemanticChars = 6000

// Scope says which requests may share a semantic answer. The semantic_cache table has only
// the columns tenant_id and model, so we put everything else that changes the answer into
// the "model" column: the model as the client sent it, the temperature and max_tokens.
func Scope(clientModel string, req *provider.ChatRequest) string {
	temp := "none"
	if req.Temperature != nil {
		temp = fmt.Sprintf("%g", *req.Temperature)
	}
	return fmt.Sprintf("%s|t=%s|max=%d", clientModel, temp, req.MaxTokens)
}

// PromptText is the text we turn into a vector: the whole conversation, one line per message.
// With the roles inside, "system: be short" and "user: be short" are not the same thing, and a
// different system prompt or an earlier message changes the vector too.
// ok is false when the text is too long for the semantic cache.
func PromptText(req *provider.ChatRequest) (text string, ok bool) {
	var b strings.Builder
	for _, m := range req.Messages {
		b.WriteString(m.Role)
		b.WriteString(": ")
		b.WriteString(m.Content)
		b.WriteByte('\n')
	}
	text = b.String()
	return text, utf8.RuneCountInString(text) <= MaxSemanticChars
}
