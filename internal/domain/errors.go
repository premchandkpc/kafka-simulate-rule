package domain

import "errors"

var (
	ErrEventIDRequired        = errors.New("event id is required")
	ErrEventTypeRequired      = errors.New("event type is required")
	ErrTenantIDRequired       = errors.New("tenant id is required")
	ErrPartitionKeyRequired   = errors.New("partition key is required")
	ErrOccurredAtRequired     = errors.New("occurred at is required")
	ErrInvalidEventEnvelope   = errors.New("invalid event envelope")

	ErrRuleIDRequired         = errors.New("rule id is required")
	ErrRevisionRequired       = errors.New("revision is required")
	ErrRuleSourceRequired     = errors.New("rule source is required")
	ErrInvalidMatchMode       = errors.New("invalid match mode")
	ErrInvalidOperator        = errors.New("invalid operator")
	ErrInvalidPredicate       = errors.New("invalid predicate")
	ErrInvalidAction          = errors.New("invalid action")
	ErrDuplicateRuleID        = errors.New("duplicate rule id")
	ErrAmbiguousPriority      = errors.New("ambiguous priority")
	ErrUnknownDestination     = errors.New("unknown destination")
	ErrDestinationNotAllowed  = errors.New("destination not allowed")
	ErrRuleExceedsLimits      = errors.New("rule exceeds limits")
	ErrInvalidRuleSchema      = errors.New("invalid rule schema")
	ErrRuleNotFound           = errors.New("rule not found")
	ErrRuleNotActive          = errors.New("rule not active")

	ErrEffectIDRequired       = errors.New("effect id is required")
	ErrExecutionIDRequired    = errors.New("execution id is required")
	ErrInvalidEffectType      = errors.New("invalid effect type")
	ErrEffectNotFound         = errors.New("effect not found")
	ErrMaxAttemptsExceeded    = errors.New("max attempts exceeded")

	ErrExecutionNotFound      = errors.New("execution not found")
	ErrInvalidExecutionStatus = errors.New("invalid execution status")
	ErrExecutionAlreadyExists = errors.New("execution already exists")

	ErrInboxEntryNotFound     = errors.New("inbox entry not found")
	ErrInboxDuplicate         = errors.New("duplicate event id")
	ErrInboxInvalidStatus     = errors.New("invalid inbox status")

	ErrLeaseNotFound          = errors.New("lease not found")
	ErrLeaseExpired           = errors.New("lease expired")
	ErrLeaseOwnedByOther      = errors.New("lease owned by another worker")
	ErrFencingTokenMismatch   = errors.New("fencing token mismatch")

	ErrQuarantineNotFound     = errors.New("quarantine entry not found")
	ErrQuarantineReplayFailed = errors.New("quarantine replay failed")

	ErrActivationNotFound     = errors.New("activation not found")
	ErrActivationConflict     = errors.New("activation conflict")

	ErrBrokerFetchFailed      = errors.New("broker fetch failed")
	ErrBrokerAckFailed        = errors.New("broker ack failed")
	ErrBrokerRetryFailed      = errors.New("broker retry failed")

	ErrEffectSendFailed       = errors.New("effect send failed")
	ErrEffectDestinationDown  = errors.New("effect destination down")

	ErrDatabaseError          = errors.New("database error")
	ErrTransactionFailed      = errors.New("transaction failed")
	ErrSerializationFailed    = errors.New("serialization failed")
	ErrDeserializationFailed  = errors.New("deserialization failed")

	ErrUnauthorized           = errors.New("unauthorized")
	ErrForbidden              = errors.New("forbidden")

	ErrEvaluationTimeout      = errors.New("evaluation timeout")
	ErrEvaluationError        = errors.New("evaluation error")
	ErrFactSnapshotRequired   = errors.New("fact snapshot required")
	ErrInvalidFactPath        = errors.New("invalid fact path")
)

type ErrorClass string

const (
	ErrorClassValidation     ErrorClass = "validation"
	ErrorClassRule           ErrorClass = "rule"
	ErrorClassEvaluation     ErrorClass = "evaluation"
	ErrorClassExecution      ErrorClass = "execution"
	ErrorClassEffect         ErrorClass = "effect"
	ErrorClassBroker         ErrorClass = "broker"
	ErrorClassDatabase       ErrorClass = "database"
	ErrorClassAuthorization  ErrorClass = "authorization"
	ErrorClassQuarantine     ErrorClass = "quarantine"
	ErrorClassInternal       ErrorClass = "internal"
)

func ClassifyError(err error) ErrorClass {
	if err == nil {
		return ErrorClassInternal
	}
	switch {
	case errors.Is(err, ErrEventIDRequired),
		errors.Is(err, ErrEventTypeRequired),
		errors.Is(err, ErrTenantIDRequired),
		errors.Is(err, ErrPartitionKeyRequired),
		errors.Is(err, ErrOccurredAtRequired),
		errors.Is(err, ErrInvalidEventEnvelope):
		return ErrorClassValidation
	case errors.Is(err, ErrRuleIDRequired),
		errors.Is(err, ErrRevisionRequired),
		errors.Is(err, ErrRuleSourceRequired),
		errors.Is(err, ErrInvalidMatchMode),
		errors.Is(err, ErrInvalidOperator),
		errors.Is(err, ErrInvalidPredicate),
		errors.Is(err, ErrInvalidAction),
		errors.Is(err, ErrDuplicateRuleID),
		errors.Is(err, ErrAmbiguousPriority),
		errors.Is(err, ErrUnknownDestination),
		errors.Is(err, ErrDestinationNotAllowed),
		errors.Is(err, ErrRuleExceedsLimits),
		errors.Is(err, ErrInvalidRuleSchema),
		errors.Is(err, ErrRuleNotFound),
		errors.Is(err, ErrRuleNotActive):
		return ErrorClassRule
	case errors.Is(err, ErrEvaluationTimeout),
		errors.Is(err, ErrEvaluationError),
		errors.Is(err, ErrFactSnapshotRequired),
		errors.Is(err, ErrInvalidFactPath):
		return ErrorClassEvaluation
	case errors.Is(err, ErrExecutionNotFound),
		errors.Is(err, ErrInvalidExecutionStatus),
		errors.Is(err, ErrExecutionAlreadyExists):
		return ErrorClassExecution
	case errors.Is(err, ErrEffectIDRequired),
		errors.Is(err, ErrExecutionIDRequired),
		errors.Is(err, ErrInvalidEffectType),
		errors.Is(err, ErrEffectNotFound),
		errors.Is(err, ErrMaxAttemptsExceeded):
		return ErrorClassEffect
	case errors.Is(err, ErrBrokerFetchFailed),
		errors.Is(err, ErrBrokerAckFailed),
		errors.Is(err, ErrBrokerRetryFailed):
		return ErrorClassBroker
	case errors.Is(err, ErrInboxEntryNotFound),
		errors.Is(err, ErrInboxDuplicate),
		errors.Is(err, ErrInboxInvalidStatus),
		errors.Is(err, ErrLeaseNotFound),
		errors.Is(err, ErrLeaseExpired),
		errors.Is(err, ErrLeaseOwnedByOther),
		errors.Is(err, ErrFencingTokenMismatch),
		errors.Is(err, ErrDatabaseError),
		errors.Is(err, ErrTransactionFailed):
		return ErrorClassDatabase
	case errors.Is(err, ErrUnauthorized),
		errors.Is(err, ErrForbidden):
		return ErrorClassAuthorization
	case errors.Is(err, ErrQuarantineNotFound),
		errors.Is(err, ErrQuarantineReplayFailed):
		return ErrorClassQuarantine
	default:
		return ErrorClassInternal
	}
}

func IsRetryable(err error) bool {
	cls := ClassifyError(err)
	return cls == ErrorClassBroker || cls == ErrorClassEffect || cls == ErrorClassDatabase
}

func IsPermanent(err error) bool {
	cls := ClassifyError(err)
	return cls == ErrorClassValidation || cls == ErrorClassRule || cls == ErrorClassAuthorization
}