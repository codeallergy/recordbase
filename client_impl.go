/*
 * Copyright (c) 2022-2023 Zander Schwid & Co. LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package recordbase

import (
	"context"
	"github.com/codeallergy/recordpb"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"io"
	"sync"
)

type implClient struct {

	conn             *grpc.ClientConn
	client           recordpb.RecordServiceClient

	nextHandle       atomic.Int64
	cancelFunctions  sync.Map

	closeOnce    sync.Once

}

func Create(conn *grpc.ClientConn) Client {
	return &implClient{
		conn: conn,
		client: recordpb.NewRecordServiceClient(conn),
	}
}

func (t *implClient) GetCounts(ctx context.Context, in *recordpb.TenantRequest) (*recordpb.Counts, error) {

	ctx, cancel := context.WithCancel(ctx)

	handle := t.addCancelFn(cancel)
	defer func() {
		t.removeCancelFn(handle)
		cancel()
	}()

	return t.client.GetCounts(ctx, in)
}

func (t *implClient) Lookup(ctx context.Context, in *recordpb.LookupRequest) (*recordpb.RecordEntry, error) {

	ctx, cancel := context.WithCancel(ctx)

	handle := t.addCancelFn(cancel)
	defer func() {
		t.removeCancelFn(handle)
		cancel()
	}()

	return t.client.Lookup(ctx, in)
}

func (t *implClient) Search(c context.Context, in *recordpb.SearchRequest) (entries <-chan RecordEntryEvent, cancel func(), err error) {

	ctx, cancel := context.WithCancel(c)
	handle := t.addCancelFn(cancel)

	stream, err := t.client.Search(ctx, in)

	if err != nil {
		t.removeCancelFn(handle)
		return nil, nil, err
	}

	resultCh := make(chan RecordEntryEvent)

	go func() {

		defer func() {
			t.removeCancelFn(handle)
			close(resultCh)
		}()

		for ctx.Err() == nil {
			entry, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					resultCh <- RecordEntryEvent { Err: err }
				}
				break
			}
			resultCh <- RecordEntryEvent { Entry: entry }
		}
	}()

	return resultCh, func() {
		t.removeCancelFn(handle)
		cancel()
	},nil

}

func (t *implClient) Get(ctx context.Context, in *recordpb.GetRequest) (*recordpb.RecordEntry, error) {

	ctx, cancel := context.WithCancel(ctx)

	handle := t.addCancelFn(cancel)
	defer func() {
		t.removeCancelFn(handle)
		cancel()
	}()

	return t.client.Get(ctx, in)
}

func (t *implClient) Create(ctx context.Context, in *recordpb.CreateRequest) (*recordpb.CreateResponse, error) {

	ctx, cancel := context.WithCancel(ctx)
	handle := t.addCancelFn(cancel)

	defer func() {
		t.removeCancelFn(handle)
		cancel()
	}()

	return t.client.Create(ctx, in)
}

func (t *implClient) Delete(ctx context.Context, in *recordpb.DeleteRequest) error {

	ctx, cancel := context.WithCancel(ctx)
	handle := t.addCancelFn(cancel)

	defer func() {
		t.removeCancelFn(handle)
		cancel()
	}()

	_, err := t.client.Delete(ctx, in)
	return  err
}

func (t *implClient) Update(ctx context.Context, in *recordpb.UpdateRequest) error {

	ctx, cancel := context.WithCancel(ctx)
	handle := t.addCancelFn(cancel)

	defer func() {
		t.removeCancelFn(handle)
		cancel()
	}()

	_, err := t.client.Update(ctx, in)
	return  err
}

func (t *implClient) Scan(c context.Context, in *recordpb.ScanRequest) (entries <-chan RecordEntryEvent, cancel func(), err error) {

	ctx, cancel := context.WithCancel(c)
	handle := t.addCancelFn(cancel)

	stream, err := t.client.Scan(ctx, in)

	if err != nil {
		t.removeCancelFn(handle)
		return nil, nil, err
	}

	resultCh := make(chan RecordEntryEvent)

	go func() {

		defer func() {
			t.removeCancelFn(handle)
			close(resultCh)
		}()

		for ctx.Err() == nil {
			entry, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					resultCh <- RecordEntryEvent { Err: err }
				}
				break
			}
			resultCh <- RecordEntryEvent { Entry: entry }
		}
	}()

	return resultCh, func() {
		t.removeCancelFn(handle)
		cancel()
	},nil
}

func (t *implClient) AddKeyRange(ctx context.Context, in *recordpb.KeyRange) (err error) {

	ctx, cancel := context.WithCancel(ctx)
	handle := t.addCancelFn(cancel)

	defer func() {
		t.removeCancelFn(handle)
		cancel()
	}()

	_, err = t.client.AddKeyRange(ctx, in)
	return err

}

func (t *implClient) GetKeyCapacity(ctx context.Context, in *recordpb.TenantRequest) (*recordpb.KeyCapacity, error) {

	ctx, cancel := context.WithCancel(ctx)
	handle := t.addCancelFn(cancel)

	defer func() {
		t.removeCancelFn(handle)
		cancel()
	}()

	return t.client.GetKeyCapacity(ctx, in)
}

func (t *implClient) MapGet(ctx context.Context, in *recordpb.MapGetRequest) (*recordpb.MapEntry, error) {

	ctx, cancel := context.WithCancel(ctx)

	handle := t.addCancelFn(cancel)
	defer func() {
		t.removeCancelFn(handle)
		cancel()
	}()

	return t.client.MapGet(ctx, in)

}

func (t *implClient) MapPut(ctx context.Context, in *recordpb.MapPutRequest) (*recordpb.MapValue, error) {

	ctx, cancel := context.WithCancel(ctx)
	handle := t.addCancelFn(cancel)

	defer func() {
		t.removeCancelFn(handle)
		cancel()
	}()

	return t.client.MapPut(ctx, in)
}

func (t *implClient) MapRemove(ctx context.Context, in *recordpb.MapRemoveRequest) (*recordpb.MapValue, error) {

	ctx, cancel := context.WithCancel(ctx)
	handle := t.addCancelFn(cancel)

	defer func() {
		t.removeCancelFn(handle)
		cancel()
	}()

	return t.client.MapRemove(ctx, in)
}

func (t *implClient) MapRange(c context.Context, in *recordpb.MapRangeRequest) (entries <-chan MapEntryEvent, cancel func(), err error) {

	ctx, cancel := context.WithCancel(c)
	handle := t.addCancelFn(cancel)

	stream, err := t.client.MapRange(ctx, in)

	if err != nil {
		t.removeCancelFn(handle)
		return nil, nil, err
	}

	resultCh := make(chan MapEntryEvent)

	go func() {

		defer func() {
			t.removeCancelFn(handle)
			close(resultCh)
		}()

		for ctx.Err() == nil {
			entry, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					resultCh <- MapEntryEvent { Err: err }
				}
				break
			}
			resultCh <- MapEntryEvent { Entry: entry }
		}
	}()

	return resultCh, func() {
		t.removeCancelFn(handle)
		cancel()
	},nil

}

func (t *implClient) addCancelFn(cancelFn func()) int64 {
	handle := t.nextHandle.Inc()
	t.cancelFunctions.Store(handle, cancelFn)
	return handle
}

func (t *implClient) removeCancelFn(handle int64) {
	t.cancelFunctions.Delete(handle)
}

func (t *implClient) Destroy() (err error) {
	t.closeOnce.Do(func() {

		err = t.conn.Close()

		t.cancelFunctions.Range(func(key, value interface{}) bool {
			if closeFn, ok := value.(func()); ok {
				closeFn()
			}
			return true
		})

	})
	return nil
}

