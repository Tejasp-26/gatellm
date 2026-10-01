// Package usage knows what requests cost and how much each tenant spent this month.
package usage

// Price is in US dollars per 1 million tokens.
type Price struct {
	InputPer1M  float64
	OutputPer1M float64
}

// prices is looked up with "<provider>/<model>".
// Providers change their prices, so check the pricing pages now and then:
//
//	Groq:   https://console.groq.com/docs/models
//	Gemini: https://ai.google.dev/gemini-api/docs/pricing
//
// Both have free tiers. We still count the normal price, so the budget
// shows what the traffic WOULD cost on a paid plan.
var prices = map[string]Price{
	// The mock is not real, so its price is fake. It is high on purpose,
	// so you can test the budget with just a few requests.
	"mock/": {InputPer1M: 10, OutputPer1M: 10},

	// Listed on the Groq docs page.
	"groq/openai/gpt-oss-120b": {InputPer1M: 0.15, OutputPer1M: 0.60},
	"groq/openai/gpt-oss-20b":  {InputPer1M: 0.075, OutputPer1M: 0.30},

	// Approximate: the docs page did not show a number for these, update them if you know better.
	"groq/llama-3.1-8b-instant":    {InputPer1M: 0.05, OutputPer1M: 0.08},
	"groq/llama-3.3-70b-versatile": {InputPer1M: 0.59, OutputPer1M: 0.79},
}

// defaultPrice is used for any model that is not in the table (for example all Gemini models).
// It is a careful (not too cheap) guess, so an unknown model never looks free.
var defaultPrice = Price{InputPer1M: 0.50, OutputPer1M: 1.50}

// PriceFor returns the price of a model. The mock has one price for all its models.
func PriceFor(providerName, model string) Price {
	if providerName == "mock" {
		return prices["mock/"]
	}
	if p, ok := prices[providerName+"/"+model]; ok {
		return p
	}
	return defaultPrice
}

// CostUSD is the price of one request.
func CostUSD(providerName, model string, promptTokens, completionTokens int) float64 {
	p := PriceFor(providerName, model)
	return float64(promptTokens)*p.InputPer1M/1e6 + float64(completionTokens)*p.OutputPer1M/1e6
}
