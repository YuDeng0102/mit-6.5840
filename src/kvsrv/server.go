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

type Result struct {
	value string
	seq   int
}

type KVServer struct {
	mu      sync.Mutex
	db      map[string]string
	history map[int64]*Result
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.db[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	cliendId := args.ClerkId
	value, ok := kv.history[cliendId]
	if ok {
		if value.seq >= args.Seq {
			return
		}
	}
	kv.db[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	cliendId := args.ClerkId
	value, ok := kv.history[cliendId]
	if ok {
		if value.seq >= args.Seq {
			reply.Value = value.value
			return
		}
	} else {
		kv.history[cliendId] = new(Result)
	}
	reply.Value = kv.db[args.Key]
	kv.history[cliendId].seq = args.Seq
	kv.history[cliendId].value = kv.db[args.Key]
	kv.db[args.Key] += args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.db = make(map[string]string)
	kv.history = make(map[int64]*Result)
	// You may need initialization code here.

	return kv
}
