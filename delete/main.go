package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %s", err)
	}

	// Create Elasticsearch client
	esRemote, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{os.Getenv("ES_TARGET_ADDR")},
		Username:  os.Getenv("ES_TARGET_USER"),
		Password:  os.Getenv("ES_TARGET_PASS"),
	})
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %s", err)
	}

	// Get all indices in JSON format
	res, err := esRemote.Cat.Indices(
		esRemote.Cat.Indices.WithFormat("json"),
	)
	if err != nil {
		log.Fatalf("Error getting indices: %s", err)
	}
	defer res.Body.Close()

	// Parse response
	type Index struct {
		Index string `json:"index"`
	}

	var indices []Index
	if err := json.NewDecoder(res.Body).Decode(&indices); err != nil {
		log.Fatalf("Error decoding JSON: %s", err)
	}

	if len(indices) == 0 {
		fmt.Println("No indices found.")
		return
	}

	// Prepare list of indices to delete (skip system indices)
	var toDelete []string
	for _, idx := range indices {
		if strings.HasPrefix(idx.Index, ".") {
			fmt.Printf("Skipping system index: %s\n", idx.Index)
			continue
		}
		toDelete = append(toDelete, idx.Index)
	}

	if len(toDelete) == 0 {
		fmt.Println("No user indices to delete.")
		return
	}

	// Confirm deletion
	fmt.Printf("Found %d indices. Deleting...\n", len(toDelete))

	// Delete all listed indices
	delRes, err := esRemote.Indices.Delete(toDelete)
	if err != nil {
		log.Fatalf("Error deleting indices: %s", err)
	}
	defer delRes.Body.Close()

	fmt.Println("Delete response status:", delRes.Status())

	if delRes.IsError() {
		log.Fatalf("Failed to delete some indices: %s", delRes.String())
	} else {
		fmt.Println("✅ All user indices deleted successfully.")
	}
}
