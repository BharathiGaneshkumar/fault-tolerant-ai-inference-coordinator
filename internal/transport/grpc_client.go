package transport

import (
	pb "raft-inference-coordinator/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewRaftClient(address string) (pb.RaftServiceClient, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return pb.NewRaftServiceClient(conn), nil
}
