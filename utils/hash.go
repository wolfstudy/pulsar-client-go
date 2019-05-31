package utils

import (
	"github.com/spaolacci/murmur3"
	"github.com/wolfstudy/pulsar-client-go/pkg/log"
)

func JavaStringHash(s string) uint32 {
	var h uint32
	for i, size := 0, len(s); i < size; i++ {
		h = 31*h + uint32(s[i])
	}

	return h
}

func Murmur3_32Hash(s string) uint32 {
	h := murmur3.New32()
	_, err := h.Write([]byte(s))
	if err != nil {
		log.Errorf("Murmur3_32Hash error: %s", err.Error())
		return 0
	}
	// Maintain compatibility with values used in Java client
	return h.Sum32() & 0x7fffffff
}
