package gost

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestRelayHandshakeHonorsCancellation(t *testing.T) {
	for _, readRequest := range []bool{false, true} {
		name := "写入请求时取消"
		if readRequest {
			name = "等待响应时取消"
		}
		t.Run(name, func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()
			if readRequest {
				go func() { _, _ = io.Copy(io.Discard, server) }()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			err := exchangeRelayRequest(ctx, client, relayCmdConnect, "127.0.0.1:80", relayNetworkTCP, "user", "password")
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("期望超时错误，实际为 %v", err)
			}
		})
	}
}

func TestRelayHandshakeKeepsSuccessfulConnectionOpen(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	serverResult := make(chan error, 1)
	go func() {
		var header [4]byte
		if _, err := io.ReadFull(server, header[:]); err != nil {
			serverResult <- err
			return
		}
		payload := make([]byte, binary.BigEndian.Uint16(header[2:]))
		if _, err := io.ReadFull(server, payload); err != nil {
			serverResult <- err
			return
		}
		_, err := server.Write([]byte{relayVersion1, relayStatusOK, 0, 0})
		serverResult <- err
	}()
	ctx, cancel := context.WithCancel(context.Background())
	if err := exchangeRelayRequest(ctx, client, relayCmdConnect, "127.0.0.1:80", relayNetworkTCP, "user", "password"); err != nil {
		t.Fatal(err)
	}
	if err := <-serverResult; err != nil {
		t.Fatal(err)
	}
	cancel()
	go func() { _, _ = server.Write([]byte("ok")) }()
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var message [2]byte
	if _, err := io.ReadFull(client, message[:]); err != nil || string(message[:]) != "ok" {
		t.Fatalf("完成握手后连接不可用: %q %v", message, err)
	}
}
