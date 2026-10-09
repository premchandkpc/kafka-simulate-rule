package quarantine

import (
	"context"
	"fmt"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

// Service handles quarantine operations
type Service struct {
	repo ports.QuarantineRepository
}

func NewService(repo ports.QuarantineRepository) *Service {
	return &Service{repo: repo}
}

// Save records a new quarantine entry
func (s *Service) Save(ctx context.Context, entry *domain.QuarantineEntry) error {
	if entry.ID == "" {
		entry.ID = domain.NewID()
	}
	return s.repo.Save(ctx, entry)
}

// Get retrieves a quarantine entry by ID
func (s *Service) Get(ctx context.Context, id string) (*domain.QuarantineEntry, error) {
	return s.repo.Get(ctx, id)
}

// List returns quarantine entries matching the filter
func (s *Service) List(ctx context.Context, filter ports.QuarantineFilter) ([]*domain.QuarantineEntry, error) {
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	return s.repo.List(ctx, filter)
}

// Replay marks a quarantine entry as replayed (for audit trail)
func (s *Service) Replay(ctx context.Context, id string) error {
	return s.repo.Replay(ctx, id)
}

// Delete removes a quarantine entry
func (s *Service) Delete(ctx context.Context, id string) error {
	return s.repo.Delete(ctx, id)
}

// ReplayAndDelete replays and then deletes (for cleanup after successful replay)
func (s *Service) ReplayAndDelete(ctx context.Context, id string) error {
	if err := s.Replay(ctx, id); err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	return s.Delete(ctx, id)
}
