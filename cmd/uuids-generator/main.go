package main

import (
	"log"
	"os"
	"strings"

	"github.com/google/uuid"
)

func main() {
	n := 1000
	uuids := make([]string, n)

	for i := 0; i < n; i++ {
		uuids[i] = uuid.New().String()
	}

	content := strings.Join(uuids, "\n")

	err := os.WriteFile("uuids.txt", []byte(content), 0644)
	if err != nil {
		log.Fatalf("failed to write uuids: %v", err)
	}
}
