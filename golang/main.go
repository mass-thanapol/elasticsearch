package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/update"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/gofiber/fiber/v2"
)

type Product struct {
	Name string `json:"name,omitempty"`
	Qty  int    `json:"qty,omitempty"`
}

var ESClient *elasticsearch.Client
var ESTypedClient *elasticsearch.TypedClient

func main() {
	cfg := elasticsearch.Config{
		Addresses: []string{
			"https://localhost:9200/",
		},
		Username: "elastic",
		Password: "changeme",
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	ESClient = client
	log.Println("ESClient")
	log.Println(ESClient.Info())
	typedClient, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	ESTypedClient = typedClient
	app := fiber.New()
	app.Get("/findAll/v1", getAllProductsV1)
	app.Get("/findAll/v2", getAllProductsV2)
	app.Get("/findById/v1/:id", getProductByIDV1)
	app.Get("/findById/v2/:id", getProductByIDV2)
	app.Post("/findByQuery/v1", getProductByQueryV1)
	app.Post("/findByQuery/v2", getProductByQueryV2)
	app.Post("/createProduct/v1", createProductV1)
	app.Post("/createProduct/v2", createProductV2)
	app.Put("/updateProduct/v1/:id", updateProductV1)
	app.Put("/updateProduct/v2/:id", updateProductV2)
	app.Delete("/deleteProductById/v1/:id", deleteProductV1)
	app.Delete("/deleteProductById/v2/:id", deleteProductV2)
	app.Listen(":3000")
}

func countTotalProducts(c *fiber.Ctx) int64 {
	var totalCount int64
	res, err := ESTypedClient.Count().Index("products").Do(context.TODO())
	if err == nil {
		totalCount = res.Count
	}
	return totalCount
}

func getAllProductsV1(c *fiber.Ctx) error {
	res, err := ESClient.Search(
		ESClient.Search.WithIndex("products"),
		ESClient.Search.WithSize(int(countTotalProducts(c))),
	)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	defer res.Body.Close()
	if res.IsError() {
		return c.Status(res.StatusCode).JSON(fiber.Map{
			"error": res.Status(),
		})
	}
	var responseMap map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&responseMap); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	hits, found := responseMap["hits"].(map[string]interface{})
	if !found {
		return c.JSON([]interface{}{})
	}
	hitsArray, found := hits["hits"].([]interface{})
	if !found {
		return c.JSON([]interface{}{})
	}
	return c.JSON(hitsArray)
}

func getAllProductsV2(c *fiber.Ctx) error {
	res, err := ESTypedClient.Search().Index("products").Size(int(countTotalProducts(c))).Do(context.TODO())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	return c.JSON(res.Hits.Hits)
}

func getProductByIDV1(c *fiber.Ctx) error {
	query := `{"query": {"match_phrase": {"_id": "` + c.Params("id") + `"}}}`
	res, err := ESClient.Search(
		ESClient.Search.WithIndex("products"),
		ESClient.Search.WithBody(strings.NewReader(query)),
	)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	defer res.Body.Close()
	if res.IsError() {
		return c.Status(res.StatusCode).JSON(fiber.Map{
			"error": res.Status(),
		})
	}
	var responseMap map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&responseMap); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	hits, found := responseMap["hits"].(map[string]interface{})
	if !found {
		return c.JSON([]interface{}{})
	}
	hitsArray, found := hits["hits"].([]interface{})
	if !found {
		return c.JSON([]interface{}{})
	}
	var hitsData interface{}
	if len(hitsArray) > 0 {
		hitsData = hitsArray[0]
	}
	return c.JSON(hitsData)
}

func getProductByIDV2(c *fiber.Ctx) error {
	res, err := ESTypedClient.Search().
		Index("products").
		Request(&search.Request{
			Query: &types.Query{MatchPhrase: map[string]types.MatchPhraseQuery{
				"_id": {
					Query: c.Params("id"),
				},
			}},
		}).
		Do(context.TODO())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	var hitsData interface{}
	if len(res.Hits.Hits) > 0 {
		hitsData = res.Hits.Hits[0]
	}
	return c.JSON(hitsData)
}

func getProductByQueryV1(c *fiber.Ctx) error {
	var query interface{}
	if err := c.BodyParser(&query); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"error": "Failed to parse request body",
		})
	}
	jsonBytes, err := json.Marshal(query)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	jsonText := string(jsonBytes)
	res, err := ESClient.Search(
		ESClient.Search.WithIndex("products"),
		ESClient.Search.WithBody(strings.NewReader(jsonText)),
	)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	defer res.Body.Close()
	if res.IsError() {
		return c.Status(res.StatusCode).JSON(fiber.Map{
			"error": res.Status(),
		})
	}
	var responseMap map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&responseMap); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	hits, found := responseMap["hits"].(map[string]interface{})
	if !found {
		return c.JSON([]interface{}{})
	}
	hitsArray, found := hits["hits"].([]interface{})
	if !found {
		return c.JSON([]interface{}{})
	}
	return c.JSON(hitsArray)
}

func getProductByQueryV2(c *fiber.Ctx) error {
	return c.JSON("Waiting for implementation ...")
}

func createProductV1(c *fiber.Ctx) error {
	var product Product
	if err := c.BodyParser(&product); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"error": "Failed to parse request body",
		})
	}
	jsonBytes, err := json.Marshal(product)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	res, err := ESClient.Index("products", bytes.NewReader(jsonBytes))
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	defer res.Body.Close()
	if res.IsError() {
		return c.Status(res.StatusCode).JSON(fiber.Map{
			"error": res.Status(),
		})
	}
	var responseMap map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&responseMap); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	return c.JSON(responseMap)
}

func createProductV2(c *fiber.Ctx) error {
	var product Product
	if err := c.BodyParser(&product); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"error": "Failed to parse request body",
		})
	}
	res, err := ESTypedClient.Index("products").
		Request(product).
		Do(context.TODO())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	return c.JSON(res)
}

func updateProductV1(c *fiber.Ctx) error {
	var product Product
	if err := c.BodyParser(&product); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"error": "Failed to parse request body",
		})
	}
	jsonBytes, err := json.Marshal(product)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	jsonText := string(jsonBytes)
	jsonText = `{"doc": ` + jsonText + `}`
	res, err := ESClient.Update("products", c.Params("id"), strings.NewReader(jsonText))
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	defer res.Body.Close()
	if res.IsError() {
		return c.Status(res.StatusCode).JSON(fiber.Map{
			"error": res.Status(),
		})
	}
	var responseMap map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&responseMap); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	return c.JSON(responseMap)
}

func updateProductV2(c *fiber.Ctx) error {
	var product Product
	if err := c.BodyParser(&product); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"error": "Failed to parse request body",
		})
	}
	jsonBytes, err := json.Marshal(product)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	jsonText := string(jsonBytes)
	res, err := ESTypedClient.Update("products", c.Params("id")).
		Request(&update.Request{
			Doc: json.RawMessage(jsonText),
		}).Do(context.TODO())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	return c.JSON(res)
}

func deleteProductV1(c *fiber.Ctx) error {
	res, err := ESClient.Delete("products", c.Params("id"))
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	defer res.Body.Close()
	if res.IsError() {
		return c.Status(res.StatusCode).JSON(fiber.Map{
			"error": res.Status(),
		})
	}
	var responseMap map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&responseMap); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	if responseMap["result"] != "deleted" {
		return c.Status(404).JSON(fiber.Map{
			"error": "Data not found",
		})
	}
	return c.JSON(responseMap)
}

func deleteProductV2(c *fiber.Ctx) error {
	res, err := ESTypedClient.Delete("products", c.Params("id")).Do(context.TODO())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	if res.Result.String() != "deleted" {
		return c.Status(404).JSON(fiber.Map{
			"error": "Data not found",
		})
	}
	return c.JSON(res)
}
