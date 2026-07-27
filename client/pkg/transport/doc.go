// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package transport implements various HTTP transport utilities based on Go
// net package.
//
// # Hot reloading of trust roots
//
// TLSInfo supports optional hot reloading of the trust-root bundle used for
// peer verification. Callers opt in by passing a CertPoolProvider to
// SetDynamicTrustRoots before ServerConfig or ClientConfig is invoked:
//
//	info := &transport.TLSInfo{...}
//	info.SetDynamicTrustRoots(myProvider)
//	cfg, err := info.ServerConfig() // cfg re-evaluates trust roots per handshake
//
// CertPoolProvider is source-agnostic: a file-watcher, SPIFFE Workload API
// client, or cert-manager informer are all valid implementations. Without a
// provider the configuration behaves identically to the previous release.
//
// This package does not provide a built-in file-backed provider adapter or
// wire the dynamic path into embed.Config / CLI flags. Those are tracked as
// separate concerns.
package transport
