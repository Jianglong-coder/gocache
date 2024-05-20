package gocache

type client struct {
	name string
}

func NewClient(peerAddr string) *client {
	return &client{name: peerAddr}
}

func (c *client) Fetch(group string, key string) ([]byte, error) {
	return []byte{}, nil
}
