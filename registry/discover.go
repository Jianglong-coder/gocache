package registry

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/resolver"
	"google.golang.org/grpc"
)

// EtcdDial 向grpc请求一个服务
// 通过提供一个etcd client和service name即可获得Connection
func EtcdDial(c *clientv3.Client, service string) (*grpc.ClientConn, error) {
	// 用etcd客户端对象创建一个grpc解析器  gRPC 的解析器用于解析目标服务的名称或地址，并将其解析为实际的连接信息。
	etcdResolver, err := resolver.NewBuilder(c)
	if err != nil {
		return nil, err
	}
	// 返回grpc客户端连接对象
	return grpc.NewClient(
		// 表示gRPC连接的目标地址。这里使用了一个硬编码的前缀"etcd:///"来指定这个连接是一个基于etcd的服务发现机制。
		// 后面拼接了传入的service参数，构成完整的连接地址。
		"etcd:///"+service,
		// 这个参数是一个选项，用于指定gRPC连接应该使用的解析器。
		// 这里将之前创建的etcdResolver解析器构建器作为参数传入，以便gRPC能够使用etcd作为服务发现机制。
		grpc.WithResolvers(etcdResolver),
		// grpc不使用TLS
		grpc.WithInsecure(),
		// grpc阻塞模式
		grpc.WithBlock(),
	)
}
