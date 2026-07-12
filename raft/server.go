package raft

// Server is the transparent test container around a Node. It deliberately keeps
// the transport tiny: the course focuses on Raft state transitions, while the
// harness owns topology, disconnects, reconnects, and trace capture.
type Server struct {
	id   int
	node *Node
	net  *network
}

func newServer(id int, peerIDs []int, net *network, ready <-chan struct{}, seed int64, trace *Trace) *Server {
	s := &Server{id: id, net: net}
	s.node = newNode(id, peerIDs, &nodeCaller{fromID: id, net: net}, ready, seed, trace)
	net.addNode(s.node)
	return s
}

func (s *Server) Status() Status {
	return s.node.Status()
}

func (s *Server) Stop() {
	s.node.Stop()
}
