// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"time"

	"cloud.google.com/go/storage"
	"github.com/gardener/gardener-extension-provider-gcp/pkg/gcp"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	errCodeBucketAlreadyOwnedByYou = 409
)

// ImmutableSettings holds configuration for an immutable backup policy.
type ImmutableSettings struct {
	RetentionType   string        `json:"retentionType"`
	RetentionPeriod time.Duration `json:"-"`
}

// ImmutableSettingsJSON is used for initial JSON unmarshaling.
// This struct reflects the exact JSON structure.
type ImmutableSettingsJSON struct {
	RetentionType   string `json:"retentionType"`
	RetentionPeriod string `json:"retentionPeriod"`
}

// ParseImmutableSettings decodes providerConfig into ImmutableSettings.
func ParseImmutableSettings(providerConfig *runtime.RawExtension) (*ImmutableSettings, error) {
	if providerConfig == nil || len(providerConfig.Raw) == 0 {
		return nil, errors.New("providerConfig is either empty or nil")
	}

	var config struct {
		ImmutableSettings ImmutableSettingsJSON `json:"immutableSettings"`
	}

	err := json.Unmarshal(providerConfig.Raw, &config)
	if err != nil {
		return nil, fmt.Errorf("error while parsing immutable settings: %v", err)
	}

	if config.ImmutableSettings.RetentionType != "bucket" {
		return nil, fmt.Errorf("invalid retentionType '%s'; must be 'bucket'", config.ImmutableSettings.RetentionType)
	}

	duration, err := time.ParseDuration(config.ImmutableSettings.RetentionPeriod)
	if err != nil {
		return nil, fmt.Errorf("invalid retentionPeriod format: %v", err)
	}

	return &ImmutableSettings{
		RetentionType:   config.ImmutableSettings.RetentionType,
		RetentionPeriod: duration,
	}, nil
}

// StorageClient is an interface which must be implemented by GCS clients.
type StorageClient interface {
	// GCS wrappers
	CreateBucketIfNotExists(ctx context.Context, bucketName, region string, imSettings *ImmutableSettings) error
	DeleteBucketIfExists(ctx context.Context, bucketName string) error
	DeleteObjectsWithPrefix(ctx context.Context, bucketName, prefix string) error
}

type storageClient struct {
	client         *storage.Client
	serviceAccount *gcp.ServiceAccount
}

// NewStorageClient creates a new storage client from the given  serviceAccount.
func NewStorageClient(ctx context.Context, serviceAccount *gcp.ServiceAccount) (StorageClient, error) {
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON(serviceAccount.Raw), option.WithScopes(storage.ScopeFullControl))
	if err != nil {
		return nil, err
	}
	return &storageClient{
		client:         client,
		serviceAccount: serviceAccount,
	}, nil
}

// NewStorageClientFromSecretRef creates a new storage client from the given <secretRef>.
func NewStorageClientFromSecretRef(ctx context.Context, c client.Client, secretRef corev1.SecretReference) (StorageClient, error) {
	serviceAccount, err := gcp.GetServiceAccountFromSecretReference(ctx, c, secretRef)
	if err != nil {
		return nil, err
	}

	return NewStorageClient(ctx, serviceAccount)
}

func (s *storageClient) CreateBucketIfNotExists(ctx context.Context, bucketName, region string, imSettings *ImmutableSettings) error {
	var retentionPolicy *storage.RetentionPolicy
	if imSettings != nil {
		retentionPolicy = &storage.RetentionPolicy{
			RetentionPeriod: imSettings.RetentionPeriod,
			IsLocked:        true,
		}
	}

	if err := s.client.Bucket(bucketName).Create(ctx, s.serviceAccount.ProjectID, &storage.BucketAttrs{
		Name:            bucketName,
		Location:        region,
		RetentionPolicy: retentionPolicy,
		UniformBucketLevelAccess: storage.UniformBucketLevelAccess{
			Enabled: true,
		},
		SoftDeletePolicy: &storage.SoftDeletePolicy{
			RetentionDuration: 0,
		},
	}); err != nil {
		if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == errCodeBucketAlreadyOwnedByYou {
			return nil
		}
		return err
	}
	return nil
}

func (s *storageClient) DeleteBucketIfExists(ctx context.Context, bucketName string) error {
	err := s.client.Bucket(bucketName).Delete(ctx)
	return IgnoreNotFoundError(err)
}

func (s *storageClient) DeleteObjectsWithPrefix(ctx context.Context, bucketName, prefix string) error {
	bucketHandle := s.client.Bucket(bucketName)
	itr := bucketHandle.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attr, err := itr.Next()
		if err != nil {
			if err == iterator.Done {
				return nil
			}
			return err
		}
		if err := bucketHandle.Object(attr.Name).Delete(ctx); err != nil && err != storage.ErrObjectNotExist {
			return err
		}
	}
}
