package kvsrv

import (
	"log"
	"sync"
	"fmt"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
    kvs map[string]string
	client_map sync.Map
	// Your definitions here.
}

func (kv *KVServer) get_map(client_id int64, request_id int64) (string, bool){
    key := fmt.Sprintf("%d-%d", client_id, request_id)
	value, ok := kv.client_map.Load(key)
	if !ok {
		return "", false
	}
	strValue, ok := value.(string)
	return strValue, ok
}

func (kv *KVServer) save_map(client_id int64, request_id int64, old_value string) {
    key := fmt.Sprintf("%d-%d", client_id, request_id)
	kv.client_map.Store(key, old_value)
}

func (kv *KVServer) drop_map(client_id int64, request_id int64) {
	key := fmt.Sprintf("%d-%d", client_id, request_id)
	kv.client_map.Delete(key)

}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	cache, ok := kv.get_map(args.ClientId, args.RequestId)
	if ok {
		reply.Value = cache
		return
	}
	value, exists := kv.kvs[args.Key]
	if exists {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	// Your code here.
}


func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	cache, ok := kv.get_map(args.ClientId, args.RequestId)
	if ok {
		reply.Value = cache
		return
	}

	kv.mu.Lock()
	value, exists := kv.kvs[args.Key]
	kv.kvs[args.Key] = args.Value
	if exists {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	kv.mu.Unlock()
	kv.save_map(args.ClientId, args.RequestId, value)
	// Your code here.
}


func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	cache, ok := kv.get_map(args.ClientId, args.RequestId)
	if ok {
		reply.Value = cache
		return
	}

	kv.mu.Lock()
	value, exists := kv.kvs[args.Key]
	if exists {
		reply.Value = value
		kv.kvs[args.Key] = value + args.Value
	} else {
		reply.Value = ""
		kv.kvs[args.Key] = args.Value
	}
	kv.mu.Unlock()
	kv.save_map(args.ClientId, args.RequestId, value)
	// Your code here.
}


func (kv *KVServer) Report(args *ReportArgs, reply *ReportReply) {
	kv.drop_map(args.ClientId, args.RequestId)
}


func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvs = make(map[string]string)
	// You may need initialization code here.

	return kv
}
