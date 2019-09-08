// Copyright (c) 2018 The Jaeger Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package throttling

import (
	"errors"

	constants "github.com/uber/jaeger-client-go"

	"github.com/jaegertracing/jaeger/cmd/agent/app/reporter"
	"github.com/jaegertracing/jaeger/pkg/multierror"
	"github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	"github.com/jaegertracing/jaeger/thrift-gen/zipkincore"
)

var (
	errNoClientID = errors.New("Span has no client ID")
)

// throttlingReporter is a custom reporter used to implement throttling.
type throttlingReporter struct {
	throttler *Throttler
}

// NewReporter creates a new reporter.Reporter to wrap a given Throttler.
func NewReporter(throttler *Throttler) reporter.Reporter {
	return &throttlingReporter{throttler: throttler}
}

// EmitZipkinBatch implements reporter.Reporter interface. Currently does
// nothing.
func (r *throttlingReporter) EmitZipkinBatch(batch []*zipkincore.Span) error {
	return nil
}

// EmitBatch implements reporter.Reporter interface. Calculates the number of
// debug traces and deducts from the client's balance accordingly.
func (r *throttlingReporter) EmitBatch(batch *jaeger.Batch) error {
	process := batch.GetProcess()
	serviceName := process.GetServiceName()
	var clientUUID string
	tag := findTag(process.GetTags(), constants.TracerUUIDTagKey)
	if tag == nil || tag.GetVStr() == "" {
		return errNoClientID
	}
	clientUUID = tag.GetVStr()
	var errors []error
	for _, span := range batch.GetSpans() {
		const (
			creditSpent = 1 // One root span = one credit
		)
		if err := r.throttler.Spend(serviceName, clientUUID, span.GetOperationName(), creditSpent); err != nil {
			errors = append(errors, err)
		}
	}
	return multierror.Wrap(errors)
}

// findTag finds a tag in an array of tags that matches the given key.
// Returns nil if no matching tag is found.
func findTag(tags []*jaeger.Tag, key string) *jaeger.Tag {
	for _, tag := range tags {
		if tag.GetKey() == key {
			return tag
		}
	}
	return nil
}
