package net

import (
	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
)

const BACKLOG int = 128

func Accept(fd int) (int, error) {
	nfd, _, err := unix.Accept(fd)
	return nfd, err
}

func Read(fd int, buf []byte) (int, error) {
	return unix.Read(fd, buf)
}

func Write(fd int, buf []byte) (int, error) {
	return unix.Write(fd, buf)
}

func Close(fd int) {
	unix.Close(fd)
}

func TcpServer(port int, logger *zerolog.Logger) (int, error) {
	s, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		logger.Error().Err(err).Msg("init socket failed")
		return -1, nil
	}
	err = unix.SetsockoptInt(s, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	if err != nil {
		logger.Error().Err(err).Msg("set SO_REUSEADDR failed")
		unix.Close(s)
		return -1, nil
	}
	var addr unix.SockaddrInet4
	addr.Port = port
	err = unix.Bind(s, &addr)
	if err != nil {
		logger.Error().Err(err).Msg("bind addr failed")
		unix.Close(s)
		return -1, nil
	}
	err = unix.Listen(s, BACKLOG)
	if err != nil {
		logger.Error().Err(err).Msg("listen socket failed")
		unix.Close(s)
		return -1, nil
	}
	return s, nil
}
