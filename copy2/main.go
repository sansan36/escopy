package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/joho/godotenv"
)

type Hit struct {
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %s", err)
	}

	esSrc, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{os.Getenv("ES_SOURCE_ADDR")},
		Username:  os.Getenv("ES_SOURCE_USER"),
		Password:  os.Getenv("ES_SOURCE_PASS"),
	})
	if err != nil {
		log.Fatalf("Error creating source client: %s", err)
	}

	esDst, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{os.Getenv("ES_TARGET_ADDR")},
		Username:  os.Getenv("ES_TARGET_USER"),
		Password:  os.Getenv("ES_TARGET_PASS"),
	})
	if err != nil {
		log.Fatalf("Error creating target client: %s", err)
	}

	sourceIndex := "re-unclaim-search-2"
	targetIndex := "unclaim-search-2"
	bulkLimitMB, _ := strconv.Atoi(os.Getenv("BULK_LIMIT_MB"))
	concurrency, _ := strconv.Atoi(os.Getenv("CONCURRENCY"))
	bulkLimitBytes := bulkLimitMB * 1024 * 1024

	fmt.Printf("Copying index [%s] → [%s]\n", sourceIndex, targetIndex)

	scrollID := ""
	page := 0
	var wg sync.WaitGroup
	docChan := make(chan []Hit, concurrency)

	// Worker goroutines for bulk insert
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for hits := range docChan {
				if len(hits) == 0 {
					continue
				}
				err := bulkInsert(esDst, targetIndex, hits, bulkLimitBytes)
				if err != nil {
					log.Printf("[Worker %d] Bulk insert error: %v", workerID, err)
				} else {
					log.Printf("[Worker %d] Inserted %d docs", workerID, len(hits))
				}
			}
		}(i)
	}

	// Start scroll API from source
	for {
		var res *esapiResponse
		if scrollID == "" {
			res, err = scroll(esSrc, sourceIndex, "")
		} else {
			res, err = scroll(esSrc, "", scrollID)
		}
		if err != nil {
			log.Fatalf("Error fetching scroll: %v", err)
		}
		if res == nil || len(res.Hits.Hits) == 0 {
			log.Println("No more documents.")
			break
		}

		page++
		log.Printf("Fetched page %d: %d docs", page, len(res.Hits.Hits))
		scrollID = res.ScrollID

		docChan <- res.Hits.Hits
	}

	close(docChan)
	wg.Wait()
	log.Println("✅ Copy complete.")
}

type esapiResponse struct {
	ScrollID string `json:"_scroll_id"`
	Hits     struct {
		Hits []Hit `json:"hits"`
	} `json:"hits"`
}

func scroll(es *elasticsearch.Client, index, scrollID string) (*esapiResponse, error) {
	var res *esapiResponse
	var r io.ReadCloser

	if scrollID == "" {
		resp, err := es.Search(
			es.Search.WithIndex(index),
			es.Search.WithScroll(time.Minute),
			es.Search.WithSize(500),
			es.Search.WithSort("_doc"),
			es.Search.WithContext(context.Background()),
		)
		if err != nil {
			return nil, err
		}
		r = resp.Body
	} else {
		resp, err := es.Scroll(
			es.Scroll.WithScrollID(scrollID),
			es.Scroll.WithScroll(time.Minute),
			es.Scroll.WithContext(context.Background()),
		)
		if err != nil {
			return nil, err
		}
		r = resp.Body
	}

	defer r.Close()
	if err := json.NewDecoder(r).Decode(&res); err != nil {
		return nil, fmt.Errorf("decode scroll response: %w", err)
	}
	return res, nil
}

func bulkInsert(es *elasticsearch.Client, index string, hits []Hit, bulkLimit int) error {
	var buf bytes.Buffer
	for _, hit := range hits {
		meta := fmt.Sprintf(`{ "index": { "_index": "%s", "_id": "%s" } }%s`, index, hit.ID, "\n")
		doc, _ := json.Marshal(hit.Source)
		buf.WriteString(meta)
		buf.Write(doc)
		buf.WriteString("\n")

		if buf.Len() >= bulkLimit {
			if err := doBulk(es, &buf); err != nil {
				return err
			}
			buf.Reset()
		}
	}

	if buf.Len() > 0 {
		return doBulk(es, &buf)
	}
	return nil
}

func doBulk(es *elasticsearch.Client, buf *bytes.Buffer) error {
	for attempt := 1; attempt <= 5; attempt++ {
		res, err := es.Bulk(bytes.NewReader(buf.Bytes()))
		if err != nil {
			wait := time.Duration(attempt*2) * time.Second
			log.Printf("Bulk attempt %d failed, retrying in %s: %v", attempt, wait, err)
			time.Sleep(wait)
			continue
		}
		defer res.Body.Close()
		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			return fmt.Errorf("bulk response error: %s", body)
		}
		return nil
	}
	return fmt.Errorf("bulk failed after max retries")
}
