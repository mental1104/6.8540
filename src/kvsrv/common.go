package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	RequestId int64
	ClientId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}


type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	RequestId int64
	ClientId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}

type ReportArgs struct {
    RequestId int64
	ClientId int64
}

type ReportReply struct {
	RequestId int64
	ClientId int64
}