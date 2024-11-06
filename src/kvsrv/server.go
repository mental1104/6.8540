package kvsrv

import (
	"log"
	"sync"
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
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exists := kv.kvs[args.Key]
	if exists {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	// Your code here.
}


func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exists := kv.kvs[args.Key]
	kv.kvs[args.Key] = args.Value
	if exists {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	// Your code here.
}


func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exists := kv.kvs[args.Key]
	if exists {
		reply.Value = value
		kv.kvs[args.Key] = value + args.Value
	} else {
		reply.Value = ""
		kv.kvs[args.Key] = args.Value
	}
	// Your code here.
}


func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvs = make(map[string]string)
	// You may need initialization code here.

	return kv
}
