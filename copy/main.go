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
		"re-unclaim-search-2",
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
				query := map[string]interface{}{
					"query": map[string]interface{}{
						"terms": map[string]interface{}{
							"header_id": []int{6891,6892,6894,6895,6896,6897,7223,7224,7225,7226,7227,7228,7229,7230,7231,7232,7233,7234,7235,7236,7237,7238,7239,7241,7242,7243,7244,7245,7246,7247,7248,7249,7250,7251,7252,7253,7254,7255,6774,6775,6776,6777,6778,6779,6780,6788,6817,6818,6819,6820,6821,6822,6789,6790,6791,6792,6793,6794,6795,6796,6802,6823,6824,6825,6826,6827,6803,6805,6806,6807,6808,6809,6828,6810,6811,6812,6813,6814,6815,6816,6650,6651,6652,6653,6654,6655,6656,6630,6631,6632,6633,6634,6657,6658,6635,6636,6637,6638,6639,6640,6641,6642,6643,6644,6645,6646,6647,6648,6659,6660,6687,6688,6689,6690,6691,6670,6671,6672,6692,6693,6694,6695,6673,6674,6675,6676,6677,6678,6679,6680,6681,6682,6683,6684,6685,6686,6696,6697,6698,6699,6700,6701,6702,6703,6704,6705,6706,6707,6708,6709,6710,6711,6712,6713,6714,6715,6716,6717,6718,6719,6720,6721,6722,6723,6491,6492,6493,6494,6495,6496,6497,6498,6499,6500,6501,6502,6503,6504,6505,6506,6507,6508,6509,6510,6511,6558,6559,6561,6562,6563,6564,6947,6565,6566,6567,6568,6569,6570,6571,6572,6573,6574,6575,6576,6577,6578,6579,6580,6581,6582,6583,6584,6585,6586,6587,6588,6589,6590,6591,6592,6593,6594,6595,6596,6597,6598,6599,6600,6601,6602,6603,6604,6605,6606,6607,6608,6609,6610,6611,6612,6613,6614,6615,6616,6617,6618,6619,6649},
						},
					},
				}
				processIndex(esLocal, esRemote, index, &failedCounts, query)
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

func processIndex(esLocal, esRemote *elasticsearch.Client, sourceIndex string, failedCounts *sync.Map, query map[string]interface{}) {
	targetIndex := sourceIndex
	log.Printf("🚀 Menyalin index: %s", sourceIndex)

	total := countDocuments(esLocal, sourceIndex, failedCounts, query)
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

	copyDocuments(esLocal, esRemote, sourceIndex, targetIndex, failedCounts, query)
}

func countDocuments(es *elasticsearch.Client, index string, failedCounts *sync.Map, query map[string]interface{}) int {
	queryJson, _ := json.Marshal(query)
	res, err := es.Count(
		es.Count.WithIndex(index),
		es.Count.WithBody(bytes.NewReader(queryJson)),
	)
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

func copyDocuments(esLocal, esRemote *elasticsearch.Client, sourceIndex, targetIndex string, failedCounts *sync.Map, query map[string]interface{}) {
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
			queryJSON, _ := json.Marshal(query)
			res, err = esLocal.Search(
				esLocal.Search.WithIndex(sourceIndex),
				esLocal.Search.WithScroll(1*time.Minute),
				esLocal.Search.WithSize(getEnvAsInt("QUERY_SIZE", 100)),
				esLocal.Search.WithBody(bytes.NewReader(queryJSON)),
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

	if err := tryBulkWithRetry(es, targetIndex, bulkBody.String(), 2); err != nil {
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
