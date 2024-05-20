package utils

import (
	"fmt"
	"net"
	"strconv"
)

func ValidPeerAddr(addr string) bool {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		fmt.Println("Error splitting host and port:", err)
		return false
	}

	// Validate the IP address
	ip := net.ParseIP(host)
	if ip == nil {
		fmt.Println("Invalid IP address:", host)
		return false
	}

	// Validate the port
	port, err := strconv.Atoi(portStr)
	if err != nil {
		fmt.Println("Error converting port:", err)
		return false
	}
	if port < 0 || port > 65535 {
		fmt.Println("Port out of range:", port)
		return false
	}

	return true
}
