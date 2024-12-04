// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package validator

import (
	"context"
	"fmt"

	extensionswebhook "github.com/gardener/gardener/extensions/pkg/webhook"
	"github.com/gardener/gardener/pkg/apis/core"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener-extension-provider-gcp/pkg/admission"
	"github.com/gardener/gardener-extension-provider-gcp/pkg/apis/gcp"
	gcpvalidation "github.com/gardener/gardener-extension-provider-gcp/pkg/apis/gcp/validation"
)

// NewSeedValidator returns a new Validator for Seed resources,
// ensuring backup configuration immutability according to policy.
func NewSeedValidator(mgr manager.Manager) extensionswebhook.Validator {
	return &seedValidator{
		client:         mgr.GetClient(),
		decoder:        serializer.NewCodecFactory(mgr.GetScheme(), serializer.EnableStrict).UniversalDecoder(),
		lenientDecoder: serializer.NewCodecFactory(mgr.GetScheme()).UniversalDecoder(),
	}
}

// seedValidator validates create and update operations on Seed resources,
// enforcing immutability of backup configurations.
type seedValidator struct {
	client         client.Client
	decoder        runtime.Decoder
	lenientDecoder runtime.Decoder
}

// Validate validates the Seed resource during create or update operations.
// It enforces immutability policies on backup configurations to prevent
// disabling immutable settings, reducing retention periods, or changing retention types.
func (s *seedValidator) Validate(ctx context.Context, newObj, oldObj client.Object) error {
	newSeed, ok := newObj.(*core.Seed)
	if !ok {
		return fmt.Errorf("wrong object type %T for new object", newObj)
	}

	if oldObj != nil {
		oldSeed, ok := oldObj.(*core.Seed)
		if !ok {
			return fmt.Errorf("wrong object type %T for old object", oldObj)
		}
		return s.validateUpdate(ctx, oldSeed, newSeed)
	}

	return s.validateCreate(newSeed)
}

// validateCreate validates the Seed object upon creation.
// It checks if immutable settings are provided and validates them to ensure they meet the required criteria.
func (s *seedValidator) validateCreate(newSeed *core.Seed) error {
	backupBucketConfig, err := admission.DecodeSeedBackupBucketConfig(s.decoder, newSeed.Spec.Backup)
	if err != nil {
		return fmt.Errorf("error decoding BackupBucketConfig: %w", err)
	}

	allErrs := gcpvalidation.ValidateBackupBucketConfig(backupBucketConfig, field.NewPath("spec", "backup", "providerConfig"))
	if len(allErrs) > 0 {
		return fmt.Errorf("validation failed: %w", allErrs.ToAggregate())
	}

	return nil
}

// validateUpdate validates updates to the Seed resource, ensuring that immutability settings for backup buckets
// are correctly managed. It enforces constraints such as preventing the unlocking of retention policies,
// disabling immutability once locked, and reduction of retention periods when policies are locked.
func (s *seedValidator) validateUpdate(_ context.Context, oldSeed, newSeed *core.Seed) error {
	oldBackupBucketConfig, err := admission.DecodeSeedBackupBucketConfig(s.lenientDecoder, oldSeed.Spec.Backup)
	if err != nil {
		return fmt.Errorf("error decoding old BackupBucketConfig: %w", err)
	}

	if oldBackupBucketConfig == nil || oldBackupBucketConfig.Immutability == (gcp.ImmutableConfig{}) {
		return s.validateCreate(newSeed)
	}

	newBackupBucketConfig, err := admission.DecodeSeedBackupBucketConfig(s.decoder, newSeed.Spec.Backup)
	if err != nil {
		return fmt.Errorf("error decoding new BackupBucketConfig: %w", err)
	}

	allErrs := gcpvalidation.ValidateBackupBucketConfig(newBackupBucketConfig, field.NewPath("spec").Child("backup").Child("providerConfig"))
	if len(allErrs) > 0 {
		return fmt.Errorf("validation failed: %w", allErrs.ToAggregate())
	}

	if !oldBackupBucketConfig.Immutability.Locked {
		return nil
	}

	if newBackupBucketConfig == nil {
		return fmt.Errorf("immutability cannot be disabled once it is locked")
	}
	if !newBackupBucketConfig.Immutability.Locked {
		return fmt.Errorf("immutable retention policy lock cannot be unlocked once it is locked")
	}

	if newBackupBucketConfig.Immutability.RetentionPeriod.Duration < oldBackupBucketConfig.Immutability.RetentionPeriod.Duration {
		return fmt.Errorf(
			"reducing the retention period from %v to %v is prohibited when the immutable retention policy is locked",
			oldBackupBucketConfig.Immutability.RetentionPeriod.Duration,
			newBackupBucketConfig.Immutability.RetentionPeriod.Duration,
		)
	}

	return nil
}
