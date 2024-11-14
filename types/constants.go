/*
 *     Copyright 2022 The Urchin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package types

import "time"

const (
	AuthHeader             = "Authorization"
	DefaultTokenExpireTime = time.Hour * 6
	//ScowTokenUrl           = "http://100.127.0.124:30090"
)

const (
	StoragePrefix           = "urchin"
	StoragePrefixScowToken  = "urchin:storage:token:LT-scow"
	StoragePrefixScowConfig = "urchin:storage:config:LT-scow"
)

const (
	HttpMethodGet    = "get"
	HttpMethodPost   = "post"
	HttpMethodDelete = "delete"
)

const (
	StorageListTypeFile = "FILE"
	StorageListTypeDir  = "DIR"
)

const (
	// AffinitySeparator is separator of affinity.
	AffinitySeparator = "|"
)
