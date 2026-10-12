package domainservices

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

var (
	ErrInvalidInput = errors.New("invalid domain service input")
	ErrNotFound     = errors.New("domain or service not found")
	ErrConflict     = errors.New("domain or service name already exists")
)

var (
	namePattern      = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)
)

const (
	maxEventTypesPerDomain = 100
	maxDomainsPerService   = 25
)

type Service struct {
	repository ports.DomainServiceRepository
	clock      ports.Clock
	allowHTTP  bool
}

func NewService(repository ports.DomainServiceRepository, clock ports.Clock, allowHTTP bool) *Service {
	if clock == nil {
		clock = domain.SystemClock{}
	}
	return &Service{repository: repository, clock: clock, allowHTTP: allowHTTP}
}

func (s *Service) SaveDomain(ctx context.Context, tenantID string, item *domain.TenantDomain) (*domain.TenantDomain, error) {
	if item == nil {
		return nil, fmt.Errorf("%w: domain is required", ErrInvalidInput)
	}
	item.TenantID = tenantID
	item.Name = strings.TrimSpace(item.Name)
	item.EventTypes = uniqueTrimmed(item.EventTypes)
	if !validTenantID(tenantID) || !namePattern.MatchString(item.Name) || len(item.EventTypes) == 0 || len(item.EventTypes) > maxEventTypesPerDomain {
		return nil, fmt.Errorf("%w: domain name and 1-%d event_types are required", ErrInvalidInput, maxEventTypesPerDomain)
	}
	for _, eventType := range item.EventTypes {
		if !eventTypePattern.MatchString(eventType) {
			return nil, fmt.Errorf("%w: invalid event type %q", ErrInvalidInput, eventType)
		}
	}
	existingDomains, err := s.repository.ListDomains(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("check tenant domain names: %w", err)
	}
	for _, existing := range existingDomains {
		if existing.ID != item.ID && existing.Name == item.Name {
			return nil, fmt.Errorf("%w: domain name %q already exists", ErrConflict, item.Name)
		}
	}
	if item.ID == "" {
		item.ID = domain.NewID()
	} else {
		existing, err := s.repository.GetDomain(ctx, tenantID, item.ID)
		if err != nil {
			return nil, fmt.Errorf("get tenant domain before update: %w", err)
		}
		if existing == nil {
			return nil, ErrNotFound
		}
		item.CreatedAt = existing.CreatedAt
	}
	now := s.clock.Now()
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	if err := s.repository.SaveDomain(ctx, item); err != nil {
		if errors.Is(err, domain.ErrDomainServiceConflict) {
			return nil, fmt.Errorf("%w: domain name %q already exists", ErrConflict, item.Name)
		}
		return nil, fmt.Errorf("save tenant domain: %w", err)
	}
	return item, nil
}

func (s *Service) GetDomain(ctx context.Context, tenantID, domainID string) (*domain.TenantDomain, error) {
	item, err := s.repository.GetDomain(ctx, tenantID, domainID)
	if err != nil {
		return nil, fmt.Errorf("get tenant domain: %w", err)
	}
	if item == nil {
		return nil, ErrNotFound
	}
	return item, nil
}

func (s *Service) ListDomains(ctx context.Context, tenantID string) ([]*domain.TenantDomain, error) {
	items, err := s.repository.ListDomains(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list tenant domains: %w", err)
	}
	return items, nil
}

func (s *Service) DeleteDomain(ctx context.Context, tenantID, domainID string) error {
	if _, err := s.GetDomain(ctx, tenantID, domainID); err != nil {
		return err
	}
	if err := s.repository.DeleteDomain(ctx, tenantID, domainID); err != nil {
		return fmt.Errorf("delete tenant domain: %w", err)
	}
	return nil
}

func (s *Service) SaveService(ctx context.Context, tenantID string, item *domain.DomainService) (*domain.DomainService, error) {
	if item == nil {
		return nil, fmt.Errorf("%w: service is required", ErrInvalidInput)
	}
	item.TenantID = tenantID
	item.Name = strings.TrimSpace(item.Name)
	item.EndpointURL = strings.TrimSpace(item.EndpointURL)
	item.DomainIDs = uniqueTrimmed(item.DomainIDs)
	if !validTenantID(tenantID) || !namePattern.MatchString(item.Name) || len(item.DomainIDs) > maxDomainsPerService {
		return nil, fmt.Errorf("%w: service name and at most %d domain_ids are required", ErrInvalidInput, maxDomainsPerService)
	}
	if err := validateEndpoint(item.EndpointURL, s.allowHTTP); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidInput, err)
	}
	for _, domainID := range item.DomainIDs {
		if _, err := s.GetDomain(ctx, tenantID, domainID); err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, fmt.Errorf("%w: domain %q does not belong to this tenant", ErrInvalidInput, domainID)
			}
			return nil, err
		}
	}
	existingServices, err := s.repository.ListServices(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("check tenant service names: %w", err)
	}
	for _, existing := range existingServices {
		if existing.ID != item.ID && existing.Name == item.Name {
			return nil, fmt.Errorf("%w: service name %q already exists", ErrConflict, item.Name)
		}
	}
	if item.ID == "" {
		item.ID = domain.NewID()
		item.Active = true
	} else {
		existing, err := s.repository.GetService(ctx, tenantID, item.ID)
		if err != nil {
			return nil, fmt.Errorf("get tenant service before update: %w", err)
		}
		if existing == nil {
			return nil, ErrNotFound
		}
		item.CreatedAt = existing.CreatedAt
		// Ordinary updates must not silently reactivate a disabled service.
		item.Active = existing.Active
	}
	now := s.clock.Now()
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	if err := s.repository.SaveService(ctx, item); err != nil {
		if errors.Is(err, domain.ErrDomainServiceConflict) {
			return nil, fmt.Errorf("%w: service name %q already exists", ErrConflict, item.Name)
		}
		return nil, fmt.Errorf("save tenant service: %w", err)
	}
	return item, nil
}

func (s *Service) GetService(ctx context.Context, tenantID, serviceID string) (*domain.DomainService, error) {
	item, err := s.repository.GetService(ctx, tenantID, serviceID)
	if err != nil {
		return nil, fmt.Errorf("get tenant service: %w", err)
	}
	if item == nil {
		return nil, ErrNotFound
	}
	return item, nil
}

func (s *Service) ListServices(ctx context.Context, tenantID string) ([]*domain.DomainService, error) {
	items, err := s.repository.ListServices(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list tenant services: %w", err)
	}
	return items, nil
}

func (s *Service) DeleteService(ctx context.Context, tenantID, serviceID string) error {
	if _, err := s.GetService(ctx, tenantID, serviceID); err != nil {
		return err
	}
	if err := s.repository.DeleteService(ctx, tenantID, serviceID); err != nil {
		return fmt.Errorf("deactivate tenant service: %w", err)
	}
	return nil
}

func (s *Service) SubscribersForEvent(ctx context.Context, tenantID, eventType string) ([]*domain.DomainService, error) {
	items, err := s.repository.ListSubscribersForEvent(ctx, tenantID, eventType)
	if err != nil {
		return nil, fmt.Errorf("list event subscribers: %w", err)
	}
	if len(items) > 100 {
		return nil, fmt.Errorf("event %q exceeds the 100-service fan-out limit", eventType)
	}
	return items, nil
}

func validateEndpoint(raw string, allowHTTP bool) error {
	parsed, err := url.Parse(raw)
	if len(raw) > 2048 || err != nil || parsed.Hostname() == "" || !parsed.IsAbs() || parsed.Opaque != "" || parsed.User != nil || parsed.Fragment != "" {
		return errors.New("endpoint_url must be an absolute HTTP(S) URL without credentials or a fragment")
	}
	if parsed.Scheme != "https" && !(allowHTTP && parsed.Scheme == "http") {
		return errors.New("endpoint_url must use HTTPS (HTTP is allowed only in development)")
	}
	if port := parsed.Port(); port != "" {
		for _, char := range port {
			if char < '0' || char > '9' {
				return errors.New("endpoint_url port is invalid")
			}
		}
	}
	return nil
}

func validTenantID(tenantID string) bool {
	if tenantID == "" || len(tenantID) > 128 {
		return false
	}
	for _, r := range tenantID {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func uniqueTrimmed(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
