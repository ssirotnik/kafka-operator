/*
SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and kafka-operator contributors
SPDX-License-Identifier: Apache-2.0
*/

package kafka

import (
	"context"
)

type Client interface {
	ReadTopic(ctx context.Context, name string) (*Topic, error)
	CreateTopic(ctx context.Context, name string, partitions int32, replicas int16, configs map[string]string) error
	UpdateTopicPartitions(ctx context.Context, topic *Topic, partitions int32) (bool, error)
	UpdateTopicReplicas(ctx context.Context, topic *Topic, replicas int16) (bool, error)
	UpdateTopicConfig(ctx context.Context, topic *Topic, configs map[string]string) (bool, error)
	DeleteTopic(ctx context.Context, name string) error

	// ListAcls returns all ACLs currently defined on the broker for the given principal.
	ListAcls(ctx context.Context, principal string) ([]Acl, error)
	// CreateAcls creates the given ACLs for the principal (idempotent on the broker).
	CreateAcls(ctx context.Context, principal string, acls []Acl) error
	// DeleteAcls deletes the given ACLs for the principal.
	DeleteAcls(ctx context.Context, principal string, acls []Acl) error
}

// Acl is a neutral (broker-agnostic) representation of a single Kafka ACL.
// All fields are strings so the value is comparable and can be used as a map
// key for set-based diffing. Enum values mirror the api/v1alpha1 Acl* string types.
type Acl struct {
	ResourceType   string // Topic|Group|Cluster|TransactionalId
	ResourceName   string
	PatternType    string // Literal|Prefixed
	Operation      string // Read|Write|...|All
	PermissionType string // Allow|Deny
	Host           string
}

type Topic struct {
	Name        string
	Id          string
	Partitions  int32
	Replicas    int16
	Assignments map[int32][]int32
	Internal    bool
	Configs     []TopicConfig
}

type TopicConfig struct {
	Key        string
	Value      string
	Overridden bool
}
