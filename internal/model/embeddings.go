package model

import (
	"errors"
	"fmt"
)

// ErrEmbeddingDimMismatch marks an embeddings response whose vector width
// differs from the catalog-declared embedding_dim. It is a gateway
// configuration error (500-class): a wrong-width vector must fail loud, never
// be returned silently.
var ErrEmbeddingDimMismatch = errors.New("embedding dimension mismatch")

// EmbeddingsRequest is the normalized domain embeddings request. Input is the
// batch of texts to embed (a single string input becomes a one-element batch).
type EmbeddingsRequest struct {
	PublicModel string
	Model       string
	Input       []string
	RequestID   string
}

// InputChars sums the text input size as the deterministic quota signal (the
// same chars/4 approximation the shared admission pipeline uses).
func (r EmbeddingsRequest) InputChars() int {
	n := 0
	for _, s := range r.Input {
		n += len(s)
	}
	return n
}

// Embedding is one vector of the normalized embeddings response. The vector
// width is the model's declared embedding_dim.
type Embedding struct {
	Object    string
	Index     int
	Embedding []float64
}

// EmbeddingsResponse is the normalized domain embeddings response. Usage is
// input-token only: embeddings have no completion tokens.
type EmbeddingsResponse struct {
	Object string
	Model  string
	Data   []Embedding
	Usage  *Usage
}

// CheckEmbeddingDim verifies the response vector width against the
// catalog-declared embedding_dim. A declared dim of zero means undeclared and
// skips the check. Every returned vector must match the declaration; a
// mismatch fails loud as ErrEmbeddingDimMismatch instead of returning a
// wrong-width vector the caller would persist.
func CheckEmbeddingDim(declared int, resp EmbeddingsResponse) error {
	if declared <= 0 {
		return nil
	}
	for _, d := range resp.Data {
		if len(d.Embedding) != declared {
			return fmt.Errorf("%w: model returns %d-dimensional vectors, catalog declares %d",
				ErrEmbeddingDimMismatch, len(d.Embedding), declared)
		}
	}
	return nil
}
