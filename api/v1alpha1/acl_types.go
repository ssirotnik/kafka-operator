/*
SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and kafka-operator contributors
SPDX-License-Identifier: Apache-2.0
*/

package v1alpha1

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AclResourceType identifies the Kafka resource an ACL applies to.
type AclResourceType string

const (
	AclResourceTypeTopic           AclResourceType = "Topic"
	AclResourceTypeGroup           AclResourceType = "Group"
	AclResourceTypeCluster         AclResourceType = "Cluster"
	AclResourceTypeTransactionalId AclResourceType = "TransactionalId"
)

// AclPatternType identifies how the resource name is matched.
type AclPatternType string

const (
	AclPatternTypeLiteral  AclPatternType = "Literal"
	AclPatternTypePrefixed AclPatternType = "Prefixed"
)

// AclOperation is a Kafka ACL operation.
type AclOperation string

const (
	AclOperationRead            AclOperation = "Read"
	AclOperationWrite           AclOperation = "Write"
	AclOperationCreate          AclOperation = "Create"
	AclOperationDelete          AclOperation = "Delete"
	AclOperationAlter           AclOperation = "Alter"
	AclOperationDescribe        AclOperation = "Describe"
	AclOperationClusterAction   AclOperation = "ClusterAction"
	AclOperationDescribeConfigs AclOperation = "DescribeConfigs"
	AclOperationAlterConfigs    AclOperation = "AlterConfigs"
	AclOperationIdempotentWrite AclOperation = "IdempotentWrite"
	AclOperationAll             AclOperation = "All"
)

// AclPermissionType is whether an ACL grants or denies access.
type AclPermissionType string

const (
	AclPermissionTypeAllow AclPermissionType = "Allow"
	AclPermissionTypeDeny  AclPermissionType = "Deny"
)

// AclAccessType is a high-level access shortcut that expands to a set of low-level rules.
type AclAccessType string

const (
	AclAccessTypeConsume   AclAccessType = "Consume"
	AclAccessTypeProduce   AclAccessType = "Produce"
	AclAccessTypeReadWrite AclAccessType = "ReadWrite"
)

// AclSpec defines the desired state of Acl.
type AclSpec struct {
	// Reference to a secret containing the connection details (see pkg/api#ConnectionDetails)
	ConnectionSecretRef *SecretKeyReference `json:"connectionSecretRef" fallbackKeys:"value"`
	// The principal the ACLs apply to, in Kafka's full form, e.g. "User:alice".
	// +kubebuilder:validation:Pattern=`^[A-Za-z]+:.+$`
	Principal string `json:"principal"`
	// +kubebuilder:validation:MinItems=1
	Entries []AclEntry `json:"entries"`
}

// AclEntry is either a low-level Rule or a high-level Access shortcut (exactly one).
// +kubebuilder:validation:XValidation:rule="has(self.rule) != has(self.access)",message="exactly one of rule or access must be set"
type AclEntry struct {
	// A low-level, full-fidelity ACL rule.
	Rule *AclRule `json:"rule,omitempty"`
	// A high-level access shortcut that expands to one or more low-level rules.
	Access *AclAccess `json:"access,omitempty"`
}

// AclRule is a single low-level Kafka ACL.
type AclRule struct {
	// +kubebuilder:validation:Enum=Topic;Group;Cluster;TransactionalId
	// +kubebuilder:default=Topic
	ResourceType AclResourceType `json:"resourceType,omitempty"`
	// Name of the resource. Ignored for Cluster (Kafka uses the fixed name "kafka-cluster").
	ResourceName string `json:"resourceName,omitempty"`
	// +kubebuilder:validation:Enum=Literal;Prefixed
	// +kubebuilder:default=Literal
	PatternType AclPatternType `json:"patternType,omitempty"`
	// +kubebuilder:validation:Enum=Read;Write;Create;Delete;Alter;Describe;ClusterAction;DescribeConfigs;AlterConfigs;IdempotentWrite;All
	Operation AclOperation `json:"operation"`
	// +kubebuilder:validation:Enum=Allow;Deny
	// +kubebuilder:default=Allow
	PermissionType AclPermissionType `json:"permissionType,omitempty"`
	// +kubebuilder:default=*
	Host string `json:"host,omitempty"`
}

// AclAccess is a high-level shortcut granting typical consume/produce access on a topic.
type AclAccess struct {
	// +kubebuilder:validation:Enum=Consume;Produce;ReadWrite
	Type AclAccessType `json:"type"`
	// +kubebuilder:validation:MinLength=1
	Topic string `json:"topic"`
	// Consumer group for Consume/ReadWrite access; defaults to the wildcard "*".
	// +kubebuilder:default=*
	Group string `json:"group,omitempty"`
	// +kubebuilder:validation:Enum=Literal;Prefixed
	// +kubebuilder:default=Literal
	PatternType AclPatternType `json:"patternType,omitempty"`
	// +kubebuilder:default=*
	Host string `json:"host,omitempty"`
}

// AclStatus defines the observed state of Acl.
type AclStatus struct {
	CommonStatus `json:",inline"`
	State        AclState    `json:"state,omitempty"`
	Details      []AclDetail `json:"details,omitempty"`
}

type AclState string

const (
	AclStatePending    AclState = "Pending"
	AclStateInProgress AclState = "InProgress"
	AclStateReady      AclState = "Ready"
	AclStateFailed     AclState = "Failed"
	AclStateDeleting   AclState = "Deleting"
)

const (
	AclReadyReasonUnknown    = "Unknown"
	AclReadyReasonFirstSeen  = "FirstSeen"
	AclReadyReasonProcessing = "Processing"
	AclReadyReasonSynced     = "Synced"
	AclReadyReasonDeleting   = "Deleting"
	AclReadyReasonRetrying   = "Retrying"
	AclReadyReasonError      = "Error"
)

// AclDetail is a flattened snapshot of one ACL currently synchronized on the broker.
type AclDetail struct {
	ResourceType   AclResourceType   `json:"resourceType,omitempty"`
	ResourceName   string            `json:"resourceName,omitempty"`
	PatternType    AclPatternType    `json:"patternType,omitempty"`
	Operation      AclOperation      `json:"operation,omitempty"`
	PermissionType AclPermissionType `json:"permissionType,omitempty"`
	Host           string            `json:"host,omitempty"`
}

// ExpandEntries expands all entries into the canonical set of low-level rules ("desired ACLs").
// It is the single source of truth consumed by the reconciler and Kafka client when diffing.
func (s *AclSpec) ExpandEntries() []AclRule {
	var rules []AclRule
	for _, entry := range s.Entries {
		switch {
		case entry.Rule != nil:
			rule := *entry.Rule
			if rule.ResourceType == "" {
				rule.ResourceType = AclResourceTypeTopic
			}
			if rule.PatternType == "" {
				rule.PatternType = AclPatternTypeLiteral
			}
			if rule.PermissionType == "" {
				rule.PermissionType = AclPermissionTypeAllow
			}
			if rule.Host == "" {
				rule.Host = "*"
			}
			rules = append(rules, rule)
		case entry.Access != nil:
			rules = append(rules, expandAccess(*entry.Access)...)
		}
	}
	return rules
}

// expandAccess translates a high-level access shortcut into low-level Allow rules.
// See franz-go kadm docs for the operations each Kafka action requires.
func expandAccess(a AclAccess) []AclRule {
	pattern := a.PatternType
	if pattern == "" {
		pattern = AclPatternTypeLiteral
	}
	host := a.Host
	if host == "" {
		host = "*"
	}
	group := a.Group
	if group == "" {
		group = "*"
	}

	topicRule := func(op AclOperation) AclRule {
		return AclRule{
			ResourceType:   AclResourceTypeTopic,
			ResourceName:   a.Topic,
			PatternType:    pattern,
			Operation:      op,
			PermissionType: AclPermissionTypeAllow,
			Host:           host,
		}
	}
	groupReadRule := AclRule{
		ResourceType:   AclResourceTypeGroup,
		ResourceName:   group,
		PatternType:    AclPatternTypeLiteral,
		Operation:      AclOperationRead,
		PermissionType: AclPermissionTypeAllow,
		Host:           host,
	}

	produce := []AclRule{
		topicRule(AclOperationWrite),
		topicRule(AclOperationCreate),
		topicRule(AclOperationDescribe),
	}
	consume := []AclRule{
		topicRule(AclOperationRead),
		topicRule(AclOperationDescribe),
		groupReadRule,
	}

	switch a.Type {
	case AclAccessTypeProduce:
		return produce
	case AclAccessTypeConsume:
		return consume
	case AclAccessTypeReadWrite:
		return dedupeRules(append(append([]AclRule{}, produce...), consume...))
	}
	return nil
}

// dedupeRules removes exact-duplicate rules while preserving order.
func dedupeRules(rules []AclRule) []AclRule {
	seen := make(map[AclRule]struct{}, len(rules))
	out := rules[:0]
	for _, r := range rules {
		if _, ok := seen[r]; ok {
			continue
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}
	return out
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Principal",type=string,JSONPath=`.spec.principal`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=='Ready')].reason`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +genclient

// Acl is the Schema for the Acls API.
// +kubebuilder:validation:XValidation:rule="self.spec.principal == oldSelf.spec.principal",message="spec.principal is immutable"
type Acl struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec AclSpec `json:"spec,omitempty"`
	// +kubebuilder:default={"observedGeneration":-1}
	Status AclStatus `json:"status,omitempty"`
}

func (a *Acl) LoadReferences(ctx context.Context, clnt client.Client) error {
	getFallbackKeys := func(v any, fieldName string, tag string) []string {
		t := reflect.TypeOf(v)
		field, haveField := t.FieldByName(fieldName)
		if !haveField {
			panic(fmt.Sprintf("this should never happen (field %s not found in type %T)", fieldName, v))
		}
		value := field.Tag.Get(tag)
		if value == "" {
			return nil
		}
		return strings.Split(value, ",")
	}

	if a.Spec.ConnectionSecretRef != nil {
		if err := a.Spec.ConnectionSecretRef.load(ctx, clnt, a.Namespace, getFallbackKeys(a.Spec, "ConnectionSecretRef", "fallbackKeys")...); err != nil {
			return err
		}
	}

	return nil
}

// +kubebuilder:object:root=true

// AclList contains a list of Acl.
type AclList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Acl `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Acl{}, &AclList{})
}
