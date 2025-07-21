package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/joho/godotenv"
)

type Hit struct {
	ID     string          `json:"_id"`
	Source json.RawMessage `json:"_source"`
}

type SearchResult struct {
	ScrollID string `json:"_scroll_id"`
	Hits     struct {
		Hits []Hit `json:"hits"`
	} `json:"hits"`
}

type CountResponse struct {
	Count int `json:"count"`
}

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

func main() {
	var failedCounts sync.Map
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %s", err)
	}

	sourceIndices := []string{
		"index-1",
		// list index here...
	}

	esLocal, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{os.Getenv("ES_SOURCE_ADDR")},
		Username:  os.Getenv("ES_SOURCE_USER"),
		Password:  os.Getenv("ES_SOURCE_PASS"),
	})
	if err != nil {
		log.Fatalf("Error membuat client lokal: %s", err)
	}

	esRemote, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{os.Getenv("ES_TARGET_ADDR")},
		Username:  os.Getenv("ES_TARGET_USER"),
		Password:  os.Getenv("ES_TARGET_PASS"),
	})
	if err != nil {
		log.Fatalf("Error membuat client remote: %s", err)
	}

	concurrency := getEnvAsInt("CONCURRENCY_LEVEL_1", 3)
	indexChan := make(chan string)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range indexChan {
				processIndex(esLocal, esRemote, index, &failedCounts)
			}
		}()
	}

	for _, idx := range sourceIndices {
		indexChan <- idx
	}
	close(indexChan)
	wg.Wait()
	log.Println("📉 Ringkasan dokumen gagal:")
	totalGagal := int64(0)
	failedCounts.Range(func(key, value any) bool {
		index := key.(string)
		count := value.(int64)
		log.Printf("🔹 [%s] Gagal: %d dokumen", index, count)
		totalGagal += count
		return true
	})

	if totalGagal > 0 {
		log.Printf("❌ Total dokumen gagal disalin: %d", totalGagal)
	} else {
		log.Println("✅ Semua dokumen berhasil disalin tanpa error.")
	}
}

func getEnvAsInt(name string, defaultVal int) int {
	valStr := os.Getenv(name)
	if valStr == "" {
		return defaultVal
	}
	var val int
	fmt.Sscanf(valStr, "%d", &val)
	return val
}

func getEnvAsMB(name string, defaultMB int64) int64 {
	valStr := os.Getenv(name)
	if valStr == "" {
		return defaultMB * 1024 * 1024
	}
	var val float64
	fmt.Sscanf(valStr, "%f", &val)
	return int64(val * 1024 * 1024)
}

func processIndex(esLocal, esRemote *elasticsearch.Client, sourceIndex string, failedCounts *sync.Map) {
	targetIndex := sourceIndex
	log.Printf("🚀 Menyalin index: %s", sourceIndex)

	total := countDocuments(esLocal, sourceIndex, failedCounts)
	log.Printf("📊 [%s] Jumlah dokumen sumber: %d", sourceIndex, total)

	existsRes, err := esRemote.Indices.Exists([]string{targetIndex})
	if err != nil {
		log.Printf("Gagal cek index target: %s", err)
		return
	}
	existsRes.Body.Close()

	if existsRes.StatusCode == 404 {
		settings := getIndexComponent(esLocal, sourceIndex, "settings")
		mapping := getIndexComponent(esLocal, sourceIndex, "mappings")

		settingsMap := settings.(map[string]interface{})
		mappingMap := mapping.(map[string]interface{})
		indexSettings, ok := settingsMap["index"].(map[string]interface{})
		if !ok {
			log.Printf("Gagal ambil settings index %s: %s", sourceIndex, err)
			return
		}

		if _, ok := indexSettings["number_of_shards"]; !ok {
			indexSettings["number_of_shards"] = "1"
		}
		if _, ok := indexSettings["number_of_replicas"]; !ok {
			indexSettings["number_of_replicas"] = "1"
		}

		delete(indexSettings, "uuid")
		delete(indexSettings, "version")
		delete(indexSettings, "provided_name")
		delete(indexSettings, "creation_date")
		delete(indexSettings, "creation_date_string")
		delete(indexSettings, "routing")

		body := map[string]interface{}{
			"settings": map[string]interface{}{
				"index": indexSettings,
			},
			"mappings": mappingMap,
		}
		bodyJSON, _ := json.Marshal(body)

		createRes, err := esRemote.Indices.Create(targetIndex, esRemote.Indices.Create.WithBody(bytes.NewReader(bodyJSON)))
		if err != nil {
			log.Printf("Gagal membuat index target: %s", err)
			return
		}
		io.Copy(io.Discard, createRes.Body)
		createRes.Body.Close()
		log.Printf("✅ [%s] Index dibuat di target", sourceIndex)
	} else {
		log.Printf("ℹ️  [%s] Index sudah ada di target", sourceIndex)
	}

	copyDocuments(esLocal, esRemote, sourceIndex, targetIndex, failedCounts)
}

func countDocuments(es *elasticsearch.Client, index string, failedCounts *sync.Map) int {
	res, err := es.Count(es.Count.WithIndex(index))
	if err != nil {
		log.Printf("⚠️ Gagal menghitung dokumen untuk index %s: %s", index, err)
		return 0
	}
	defer res.Body.Close()
	var countRes CountResponse
	err = json.NewDecoder(res.Body).Decode(&countRes)
	if err != nil {
		log.Printf("⚠️ Gagal decode response count untuk index %s: %s", index, err)
		return 0
	}
	return countRes.Count
}

func getIndexComponent(es *elasticsearch.Client, indexName, component string) interface{} {
	res, err := es.Indices.Get([]string{indexName})
	if err != nil {
		log.Fatalf("Gagal ambil %s dari %s: %s", component, indexName, err)
	}
	defer res.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(res.Body).Decode(&result)
	data := result[indexName].(map[string]interface{})[component]
	return data
}

func copyDocuments(esLocal, esRemote *elasticsearch.Client, sourceIndex, targetIndex string, failedCounts *sync.Map) {
	scrollID := ""
	totalDocuments := 0
	batchChan := make(chan []Hit)
	var wg sync.WaitGroup
	var failedCount int64
	workerCount := getEnvAsInt("CONCURRENCY_LEVEL_2", 4)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for hits := range batchChan {
				maxSize := getEnvAsMB("MAX_BULK_SIZE", 100)
				var currentBatch []Hit
				var currentSize int

				for _, hit := range hits {
					meta := fmt.Sprintf(`{ "index" : { "_id" : "%s" } }`, hit.ID)
					entry := meta + "\n" + string(hit.Source) + "\n"
					entrySize := len(entry)

					if currentSize+entrySize > int(maxSize) && len(currentBatch) > 0 {
						sendBulk(esRemote, sourceIndex, targetIndex, currentBatch, &failedCount)
						currentBatch = []Hit{}
						currentSize = 0
					}

					currentBatch = append(currentBatch, hit)
					currentSize += entrySize
				}

				if len(currentBatch) > 0 {
					sendBulk(esRemote, sourceIndex, targetIndex, currentBatch, &failedCount)
				}
			}
		}(i)
	}

	for {
		var res *esapi.Response
		var err error
		if scrollID == "" {
			res, err = esLocal.Search(
				esLocal.Search.WithIndex(sourceIndex),
				esLocal.Search.WithScroll(1*time.Minute),
				esLocal.Search.WithSize(getEnvAsInt("QUERY_SIZE", 100)),
			)
		} else {
			res, err = esLocal.Scroll(
				esLocal.Scroll.WithScrollID(scrollID),
				esLocal.Scroll.WithScroll(1*time.Minute),
			)
		}
		if err != nil {
			log.Printf("Scroll error: %s", err)
			break
		}
		bodyBytes, _ := io.ReadAll(res.Body)
		res.Body.Close()

		var sr SearchResult
		json.Unmarshal(bodyBytes, &sr)
		scrollID = sr.ScrollID

		if len(sr.Hits.Hits) == 0 {
			break
		}

		totalDocuments += len(sr.Hits.Hits)
		batchChan <- sr.Hits.Hits
	}

	close(batchChan)
	wg.Wait()
	failedCounts.Store(sourceIndex, failedCount)
}

func sendBulk(es *elasticsearch.Client, sourceIndex, targetIndex string, hits []Hit, failedCount *int64) {
	var bulkBody strings.Builder
	for _, hit := range hits {
		meta := fmt.Sprintf(`{ "index" : { "_id" : "%s" } }`, hit.ID)
		bulkBody.WriteString(meta + "\n")
		bulkBody.Write(hit.Source)
		bulkBody.WriteString("\n")
	}
	bulkSize := len(bulkBody.String())
	log.Printf("📦 [%s] Bulk - %d dokumen, size: %s", sourceIndex, len(hits), formatBytes(bulkSize))

	if err := tryBulkWithRetry(es, targetIndex, bulkBody.String(), 5); err != nil {
		log.Printf("❌ [%s] Bulk gagal: %s", sourceIndex, err)
		atomic.AddInt64(failedCount, int64(len(hits))) // ⬅️ Tambah jumlah gagal
	}
}

func tryBulkWithRetry(es *elasticsearch.Client, indexName, bulkData string, maxRetries int) error {
	var err error
	var res *esapi.Response

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			sleep := time.Duration(math.Pow(2, float64(attempt)))*time.Second + time.Duration(rng.Intn(1000))*time.Millisecond
			log.Printf("🔁 Retry bulk ke-%d, tidur %v", attempt, sleep)
			time.Sleep(sleep)
		}

		res, err = es.Bulk(
			strings.NewReader(bulkData),
			es.Bulk.WithIndex(indexName),
		)
		if err != nil {
			log.Printf("❌ Gagal kirim bulk ke Elasticsearch: %s", err)
			continue
		}

		bodyBytes, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.IsError() {
			log.Printf("❌ Error bulk [%s] status: %s\n%s", indexName, res.Status(), string(bodyBytes))
			continue
		}

		return nil
	}

	return fmt.Errorf("bulk insert gagal setelah %d retry", maxRetries)
}

func formatBytes(bytes int) string {
	const (
		KB = 1 << 10
		MB = 1 << 20
		GB = 1 << 30
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
