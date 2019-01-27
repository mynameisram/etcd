// Copyright 2016 The etcd Authors
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

package recipe

import (
	"context"
	"fmt"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

// Queue implements a multi-reader, multi-writer distributed queue.
type Queue struct {
	client *v3.Client
	ctx    context.Context

	keyPrefix string
}

func NewQueue(client *v3.Client, keyPrefix string) *Queue {
	return &Queue{client, context.TODO(), keyPrefix}
}

func (q *Queue) Enqueue(val string) error {
	_, err := newUniqueKV(q.client, q.keyPrefix, val)
	return err
}

// Dequeue returns Enqueue()'d elements in FIFO order. If the
// queue is empty, Dequeue blocks until elements are available.
func (q *Queue) Dequeue() (string, error) {
	// TODO: fewer round trips by fetching more than one key
	resp, err := q.client.Get(q.ctx, q.keyPrefix, v3.WithFirstRev()...)
	if err != nil {
		return "", err
	}

	fmt.Printf("Dequeue. Get on %s returned %d keys on queue %p\n", q.keyPrefix, len(resp.Kvs), q)

	kv, err := claimFirstKey(q.client, resp.Kvs)
	if err != nil {
		return "", err
	} else if kv != nil {
		fmt.Printf("Dequeue. Successfully dequeued key %s and modRevision %d based on Get from queue %p\n", string(kv.Key), kv.ModRevision, q)

		return string(kv.Value), nil
	} else if resp.More {
		// missed some items, retry to read in more
		return q.Dequeue()
	}

	fmt.Printf("Dequeue. Starting to wait for events on prefix %s at revision %d on queue %p\n", q.keyPrefix, resp.Header.Revision, q)

	// nothing yet; wait on elements
	ev, err := WaitPrefixEvents(
		q.client,
		q.keyPrefix,
		resp.Header.Revision,
		[]mvccpb.Event_EventType{mvccpb.PUT})
	if err != nil {
		return "", err
	}

	fmt.Printf("Dequeue. Found event on key %s and modRevision %d, while waiting on queue %p\n", ev.Kv.Key, ev.Kv.ModRevision, q)

	ok, err := deleteRevKey(q.client, string(ev.Kv.Key), ev.Kv.ModRevision)
	if err != nil {
		return "", err
	} else if !ok {
		fmt.Printf("Dequeue. Failed to delete key %s and modRevision %d, on queue %p\n", ev.Kv.Key, ev.Kv.ModRevision, q)

		return q.Dequeue()
	}

	fmt.Printf("Dequeue. Successfully deleted key %s and modRevision %d, on queue %p\n", ev.Kv.Key, ev.Kv.ModRevision, q)

	return string(ev.Kv.Value), err
}
