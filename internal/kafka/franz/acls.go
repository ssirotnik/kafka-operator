/*
SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and kafka-operator contributors
SPDX-License-Identifier: Apache-2.0
*/

package franz

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/sap/kafka-operator/internal/kafka"
)

// Neutral string enum values (mirroring api/v1alpha1 Acl* types) mapped to kadm/kmsg.

func aclResourceTypeToKmsg(t string) (kmsg.ACLResourceType, error) {
	switch t {
	case "Topic":
		return kmsg.ACLResourceTypeTopic, nil
	case "Group":
		return kmsg.ACLResourceTypeGroup, nil
	case "Cluster":
		return kmsg.ACLResourceTypeCluster, nil
	case "TransactionalId":
		return kmsg.ACLResourceTypeTransactionalId, nil
	default:
		return 0, fmt.Errorf("unknown ACL resource type %q", t)
	}
}

func aclResourceTypeFromKmsg(t kmsg.ACLResourceType) (string, error) {
	switch t {
	case kmsg.ACLResourceTypeTopic:
		return "Topic", nil
	case kmsg.ACLResourceTypeGroup:
		return "Group", nil
	case kmsg.ACLResourceTypeCluster:
		return "Cluster", nil
	case kmsg.ACLResourceTypeTransactionalId:
		return "TransactionalId", nil
	default:
		return "", fmt.Errorf("unsupported ACL resource type %s", t)
	}
}

func aclOperationToKadm(op string) (kadm.ACLOperation, error) {
	switch op {
	case "Read":
		return kadm.OpRead, nil
	case "Write":
		return kadm.OpWrite, nil
	case "Create":
		return kadm.OpCreate, nil
	case "Delete":
		return kadm.OpDelete, nil
	case "Alter":
		return kadm.OpAlter, nil
	case "Describe":
		return kadm.OpDescribe, nil
	case "ClusterAction":
		return kadm.OpClusterAction, nil
	case "DescribeConfigs":
		return kadm.OpDescribeConfigs, nil
	case "AlterConfigs":
		return kadm.OpAlterConfigs, nil
	case "IdempotentWrite":
		return kadm.OpIdempotentWrite, nil
	case "All":
		return kadm.OpAll, nil
	default:
		return 0, fmt.Errorf("unknown ACL operation %q", op)
	}
}

func aclOperationFromKadm(op kadm.ACLOperation) (string, error) {
	switch op {
	case kadm.OpRead:
		return "Read", nil
	case kadm.OpWrite:
		return "Write", nil
	case kadm.OpCreate:
		return "Create", nil
	case kadm.OpDelete:
		return "Delete", nil
	case kadm.OpAlter:
		return "Alter", nil
	case kadm.OpDescribe:
		return "Describe", nil
	case kadm.OpClusterAction:
		return "ClusterAction", nil
	case kadm.OpDescribeConfigs:
		return "DescribeConfigs", nil
	case kadm.OpAlterConfigs:
		return "AlterConfigs", nil
	case kadm.OpIdempotentWrite:
		return "IdempotentWrite", nil
	case kadm.OpAll:
		return "All", nil
	default:
		return "", fmt.Errorf("unsupported ACL operation %s", op)
	}
}

func aclPatternToKadm(p string) (kadm.ACLPattern, error) {
	switch p {
	case "Literal":
		return kadm.ACLPatternLiteral, nil
	case "Prefixed":
		return kadm.ACLPatternPrefixed, nil
	default:
		return 0, fmt.Errorf("unknown ACL pattern type %q", p)
	}
}

func aclPatternFromKadm(p kadm.ACLPattern) (string, error) {
	switch p {
	case kadm.ACLPatternLiteral:
		return "Literal", nil
	case kadm.ACLPatternPrefixed:
		return "Prefixed", nil
	default:
		return "", fmt.Errorf("unsupported ACL pattern type %s", p)
	}
}

func aclPermissionFromKmsg(p kmsg.ACLPermissionType) (string, error) {
	switch p {
	case kmsg.ACLPermissionTypeAllow:
		return "Allow", nil
	case kmsg.ACLPermissionTypeDeny:
		return "Deny", nil
	default:
		return "", fmt.Errorf("unsupported ACL permission type %s", p)
	}
}

// aclBuilderFor constructs a single-ACL builder for create/delete of one neutral ACL.
func aclBuilderFor(principal string, acl kafka.Acl) (*kadm.ACLBuilder, error) {
	op, err := aclOperationToKadm(acl.Operation)
	if err != nil {
		return nil, err
	}
	pattern, err := aclPatternToKadm(acl.PatternType)
	if err != nil {
		return nil, err
	}

	b := kadm.NewACLs().ResourcePatternType(pattern).Operations(op)
	switch acl.ResourceType {
	case "Topic":
		b = b.Topics(acl.ResourceName)
	case "Group":
		b = b.Groups(acl.ResourceName)
	case "Cluster":
		// Kafka uses the fixed cluster resource name; ResourceName is ignored.
		b = b.Clusters()
	case "TransactionalId":
		b = b.TransactionalIDs(acl.ResourceName)
	default:
		return nil, fmt.Errorf("unknown ACL resource type %q", acl.ResourceType)
	}

	host := acl.Host
	if host == "" {
		host = "*"
	}
	switch acl.PermissionType {
	case "Allow":
		b = b.Allow(principal).AllowHosts(host)
	case "Deny":
		b = b.Deny(principal).DenyHosts(host)
	default:
		return nil, fmt.Errorf("unknown ACL permission type %q", acl.PermissionType)
	}
	return b, nil
}

func (c *client) ListAcls(ctx context.Context, principal string) ([]kafka.Acl, error) {
	// Filter: all resources, all patterns, all operations, for this principal on any host,
	// both allow and deny permissions. Empty host args mean "any host" for describe filters.
	b := kadm.NewACLs().
		AnyResource().
		ResourcePatternType(kadm.ACLPatternAny).
		Operations(kadm.OpAny).
		Allow(principal).AllowHosts().
		Deny(principal).DenyHosts()

	results, err := c.c.DescribeACLs(ctx, b)
	if err != nil {
		return nil, fmt.Errorf("error describing ACLs for principal %s: %w", principal, err)
	}

	var acls []kafka.Acl
	for _, result := range results {
		if result.Err != nil {
			if result.ErrMessage == "" {
				return nil, fmt.Errorf("error describing ACLs for principal %s: %w", principal, result.Err)
			}
			return nil, fmt.Errorf("error describing ACLs for principal %s: %w (%s)", principal, result.Err, result.ErrMessage)
		}
		for _, described := range result.Described {
			// Describe may return broader results; keep only the exact principal we asked for.
			if described.Principal != principal {
				continue
			}
			resourceType, err := aclResourceTypeFromKmsg(described.Type)
			if err != nil {
				return nil, err
			}
			pattern, err := aclPatternFromKadm(described.Pattern)
			if err != nil {
				return nil, err
			}
			operation, err := aclOperationFromKadm(described.Operation)
			if err != nil {
				return nil, err
			}
			permission, err := aclPermissionFromKmsg(described.Permission)
			if err != nil {
				return nil, err
			}
			acls = append(acls, kafka.Acl{
				ResourceType:   resourceType,
				ResourceName:   described.Name,
				PatternType:    pattern,
				Operation:      operation,
				PermissionType: permission,
				Host:           described.Host,
			})
		}
	}
	return acls, nil
}

func (c *client) CreateAcls(ctx context.Context, principal string, acls []kafka.Acl) error {
	for _, acl := range acls {
		b, err := aclBuilderFor(principal, acl)
		if err != nil {
			return err
		}
		results, err := c.c.CreateACLs(ctx, b)
		if err != nil {
			return fmt.Errorf("error creating ACL for principal %s: %w", principal, err)
		}
		for _, result := range results {
			if result.Err != nil {
				if result.ErrMessage == "" {
					return fmt.Errorf("error creating ACL for principal %s: %w", principal, result.Err)
				}
				return fmt.Errorf("error creating ACL for principal %s: %w (%s)", principal, result.Err, result.ErrMessage)
			}
		}
	}
	return nil
}

func (c *client) DeleteAcls(ctx context.Context, principal string, acls []kafka.Acl) error {
	for _, acl := range acls {
		b, err := aclBuilderFor(principal, acl)
		if err != nil {
			return err
		}
		results, err := c.c.DeleteACLs(ctx, b)
		if err != nil {
			return fmt.Errorf("error deleting ACL for principal %s: %w", principal, err)
		}
		for _, result := range results {
			if result.Err != nil {
				if result.ErrMessage == "" {
					return fmt.Errorf("error deleting ACL for principal %s: %w", principal, result.Err)
				}
				return fmt.Errorf("error deleting ACL for principal %s: %w (%s)", principal, result.Err, result.ErrMessage)
			}
			for _, deleted := range result.Deleted {
				if deleted.Err != nil {
					if deleted.ErrMessage == "" {
						return fmt.Errorf("error deleting ACL for principal %s: %w", principal, deleted.Err)
					}
					return fmt.Errorf("error deleting ACL for principal %s: %w (%s)", principal, deleted.Err, deleted.ErrMessage)
				}
			}
		}
	}
	return nil
}
