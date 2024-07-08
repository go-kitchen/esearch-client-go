package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/go-kitchen/esearch-client-go/util"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type SearchEngine struct{}
type SearchEngine_Doc interface {
	ToJSON() string
	GetID() string
	SetID(string)
	FieldsToMap() map[string]interface{}
}

var ESClient *elasticsearch.Client

func InitESClient(addrArray []string) error {
	if addrArray == nil {
		return fmt.Errorf("Empty Address list")
	}
	cfg := elasticsearch.Config{
		Addresses: addrArray,
	}
	var err error
	ESClient, err = elasticsearch.NewClient(cfg)
	fmt.Println("ES initialized...")

	if err != nil {
		fmt.Println("ES initialized error:", err)
		return err
	}
	return nil
}

type document struct {
	Source interface{} `json:"_source"`
}
type queryResponse struct {
	Took    int         `json:"took"`
	Timeout bool        `json:"timeout"`
	Hits    *hitSummary `json:"hits"`
}
type hitSummary struct {
	Total total `json:"Total"`
	Hits  []Hit `json:"hits"`
}

type Hit struct {
	Index   string          `json:"_index"`
	HitType string          `json::"_type"`
	ID      string          `json:"_id"`
	Score   float32         `json:"_score"`
	Source  json.RawMessage `json:"_source"`
}

type DocOpt struct {
	Index       string `json:"_index"`
	HitType     string `json::"_type"`
	ID          string `json:"_id"`
	Version     int    `json:"_version"`
	Result      string `json:"result"`
	Shard       Shard  `json:"_shards"`
	SeqNo       int    `json:"_seq_no"`
	PrimaryTerm int    `json:"_primary_term"`
}

type BulkOpt struct {
	Took   int     `json:"took"`
	Errors bool    `json:"errors"`
	Items  []index `json:"items"`
}

type index struct {
	Index DocOpt `json:"index"`
}

type Shard struct {
	Total      int `json:"total"`
	Successful int `json:"total"`
	Failed     int `json:"failed"`
}

type total struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type query_multiMatch struct {
	Query                           string   `json:"query"`
	Fields                          []string `json:"fields"`
	AutoGenerateSynonymsPhraseQuery bool     `json:"auto_generate_synonyms_phrase_query"`
	FuzzyTranspositions             bool     `json:"fuzzy_transpositions"`
}

func (*SearchEngine) Index(indexName string) error {
	res, err := ESClient.Indices.Exists([]string{indexName})
	if err != nil {
		fmt.Println("Creating index error:", err)
		return err
	}
	if res.StatusCode == 200 {
		return nil
	}
	if res.StatusCode != 404 {
		return fmt.Errorf("error in index existence response: %s", res.String())
	}
	res, err = ESClient.Indices.Create(indexName)
	if err != nil {
		return fmt.Errorf("cannot create index: %w", err)
	}
	if res.IsError() {
		return fmt.Errorf("error in index creation response: %s", res.String())
	}
	return nil
}

func (*SearchEngine) AddDoc(indexName string, data SearchEngine_Doc) (id string, err error) {

	if data == nil {
		return "", fmt.Errorf("Empty doc")
	}

	req := esapi.IndexRequest{
		Index:   indexName,
		Body:    bytes.NewReader([]byte(data.ToJSON())),
		Refresh: "true",
	}

	res, err := req.Do(context.Background(), ESClient)

	if err != nil {
		return "", fmt.Errorf("add doc request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return "", fmt.Errorf("add doc request 404")
	}

	if res.IsError() {
		return "", fmt.Errorf("add doc response: %s", res.String())
	}

	fmt.Println("add doc response:", res)
	var (
		body DocOpt
	)

	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("add doc decode: %w", err)
	}
	if err != nil {
		fmt.Println("Add Doc error:", err)
		return
	}
	defer res.Body.Close()
	id = body.ID

	return
}

func (*SearchEngine) AddDocs(indexName string, list []SearchEngine_Doc) (ids []string, err error) {
	for _, data := range list {
		req := esapi.IndexRequest{
			Index:   indexName,
			Body:    bytes.NewReader([]byte(data.ToJSON())),
			Refresh: "true",
		}

		res, err := req.Do(context.Background(), ESClient)

		if err != nil {
			return nil, fmt.Errorf("add doc request: %w", err)
		}
		defer res.Body.Close()

		if res.StatusCode == 404 {
			return nil, fmt.Errorf("add doc request 404")
		}

		if res.IsError() {
			return nil, fmt.Errorf("add doc response: %s", res.String())
		}

		fmt.Println("add doc response:", res)
		var (
			body DocOpt
		)

		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			return nil, fmt.Errorf("add doc decode: %w", err)
		}
		if err != nil {
			fmt.Println("Add Doc error:", err)
			continue
		}
		defer res.Body.Close()
		ids = append(ids, body.ID)
	}
	return
}

func (*SearchEngine) BulkCreate(indexName string, list []SearchEngine_Doc) (ids []string, err error) {
	if len(list) == 0 {
		return nil, fmt.Errorf("Empty docs")
	}
	action := util.MapToJson(map[string]interface{}{
		"index": map[string]interface{}{
			"_index": indexName, // Elasticsearch index name
		},
	})
	var buf strings.Builder
	for _, doc := range list {
		buf.WriteString(fmt.Sprintf("%s\n", action))
		buf.WriteString(fmt.Sprintf("%s\n", doc.ToJSON()))
	}

	// fmt.Println("BUF:", buf.String())
	req := esapi.BulkRequest{
		Index:   indexName,
		Body:    bytes.NewReader([]byte(buf.String())),
		Refresh: "true",
	}
	res, err := req.Do(context.Background(), ESClient)
	if err != nil {
		log.Fatalf("Error executing bulk request: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Error response: %s", res.String())
	}

	fmt.Println("response:", res)
	var (
		body BulkOpt
	)

	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		fmt.Println("error:", err)
		return nil, fmt.Errorf("bulk insert doc decode errror: %w", err)
	}

	defer res.Body.Close()

	for _, it := range body.Items {
		ids = append(ids, it.Index.ID)
	}

	return
}

func (*SearchEngine) BulkUpdate(indexName string, list []SearchEngine_Doc) (err error) {
	if len(list) == 0 {
		return fmt.Errorf("Empty docs")
	}

	var buf strings.Builder
	for _, doc := range list {
		action := util.MapToJson(map[string]interface{}{
			"update": map[string]interface{}{
				"_index": indexName,
				"_id":    doc.GetID(),
			},
		})
		doc := util.MapToJson(map[string]interface{}{
			"doc": doc,
		})
		buf.WriteString(fmt.Sprintf("%s\n", action))
		buf.WriteString(fmt.Sprintf("%s\n", doc))
	}

	fmt.Println("BUF:", buf.String())
	req := esapi.BulkRequest{
		Index:   indexName,
		Body:    bytes.NewReader([]byte(buf.String())),
		Refresh: "true",
	}
	res, err := req.Do(context.Background(), ESClient)
	if err != nil {
		log.Fatalf("Error executing bulk request: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Error response: %s", res.String())
	}

	fmt.Println("response:", res)
	return
}

func (*SearchEngine) UpdateDoc(indexName string, id string, fields map[string]interface{}) error {
	if strings.TrimSpace(id) == "" || fields == nil {
		return fmt.Errorf("Empty id or fiedls")
	}
	updateData := map[string]interface{}{
		"doc": fields,
	}

	req := esapi.UpdateRequest{
		Index:      indexName,
		DocumentID: id,
		Body:       bytes.NewReader([]byte(util.MapToJson(updateData))),
	}
	res, err := req.Do(context.Background(), ESClient)
	if err != nil {
		return fmt.Errorf("update request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return fmt.Errorf("update request 404")
	}

	if res.IsError() {
		return fmt.Errorf("update request: %s", res.String())
	}

	fmt.Println("update doc res:", res)

	return nil
}

func (*SearchEngine) BulkDelete(indexName string, ids []string) (err error) {
	if len(ids) == 0 {
		return fmt.Errorf("Empty ids")
	}

	var buf strings.Builder
	for _, id := range ids {
		action := util.MapToJson(map[string]interface{}{
			"delete": map[string]interface{}{
				"_index": indexName,
				"_id":    id,
			},
		})
		buf.WriteString(fmt.Sprintf("%s\n", action))
	}

	// fmt.Println("BUF:", buf.String())
	req := esapi.BulkRequest{
		Index:   indexName,
		Body:    bytes.NewReader([]byte(buf.String())),
		Refresh: "true",
	}
	res, err := req.Do(context.Background(), ESClient)
	if err != nil {
		log.Fatalf("Error executing bulk request: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Error response: %s", res.String())
	}

	fmt.Println("response:", res)

	return
}
func (*SearchEngine) DeleteDoc(indexName string, doc SearchEngine_Doc) error {
	req := esapi.DeleteRequest{
		Index:      indexName,
		DocumentID: doc.GetID(),
	}

	res, err := req.Do(context.Background(), ESClient)
	if err != nil {
		return fmt.Errorf("delete request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return fmt.Errorf("delete request 404")
	}

	if res.IsError() {
		return fmt.Errorf("delete: response: %s", res.String())
	}

	return nil
}

func (*SearchEngine) GetOne(indexName string, id string) (*Hit, error) {
	req := esapi.GetRequest{
		Index:      indexName,
		DocumentID: id,
	}
	res, err := req.Do(context.Background(), ESClient)
	if err != nil {
		return nil, fmt.Errorf("GetOne request: %w", err)
	}
	fmt.Println("res:", res)
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil, fmt.Errorf("GetOne request 404")
	}

	if res.IsError() {
		return nil, fmt.Errorf("find one: response: %s", res.String())
	}

	var (
		body Hit
	)

	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("GetOne decode: %w", err)
	}

	return &body, nil
}

func (r *SearchEngine) QueryByIDs(indexName string, ids []string) ([]Hit, error) {
	qt := `{
		"query": {
		   "ids":
			  %s
		}
	}`
	v := struct {
		Values []string `json:"values"`
	}{Values: ids}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	q := fmt.Sprintf(qt, string(b))
	fmt.Println("query:", q)
	return r.executeQuery(indexName, q, 0, 100)

}

// func (r *SearchEngine) QuerybyTermRange(indexName string, field string, values []string, t reflect.Type) ([]interface{}, error) {
// 	qt := `{
// 		"query": {
// 		   "range":{
// 			   "gte":[%s]
// 		   }
// 		}
// 	}`
// 	q := fmt.Sprintf(qt, field, strings.Join(values, ","))
// 	return r.executeQuery(indexName, q, t)

// }

func (r *SearchEngine) QueryByTerms(indexName string, field string, values []string, t reflect.Type) ([]Hit, error) {
	qt := `{
		"query": {
		   "terms":%s
		}
	}`
	v := map[string][]string{
		field: values,
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	q := fmt.Sprintf(qt, string(b))
	return r.executeQuery(indexName, q, 0, 100)

}

func (r *SearchEngine) FilterQuery(indexName string, filters map[string]interface{}) ([]Hit, error) {
	qt := `{
		"query": {
		   "bool":{
			"filter":%s
		   }
		}
	}`

	type term struct {
		M map[string]interface{} `json:"term"`
	}

	var l []term

	for k, v := range filters {
		st := struct {
			M map[string]interface{} `json:"term"`
		}{
			M: map[string]interface{}{
				k: v,
			},
		}

		l = append(l, st)
	}

	b, err := json.Marshal(l)
	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf(qt, string(b))
	return r.executeQuery(indexName, q, 0, 100)

}

func (r *SearchEngine) MultiQuery(indexName string, fields []string, text string, t reflect.Type) ([]Hit, error) {
	qt := `{
		"query": {
			"multi_match": %s
		}
	}`

	v := query_multiMatch{Query: text, Fields: fields}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	q := fmt.Sprintf(qt, string(b))

	return r.executeQuery(indexName, q, 0, 100)
}

func (r *SearchEngine) Query2(indexName string, value string, text string) ([]Hit, error) {

	qt := `{
		"query": {
			"match": {
				"%s": {
					"query":"%s",
					"fuzziness":"AUTO",
					"operator":"AND"
				}
			}
		}
	}`

	q := fmt.Sprintf(qt, value, text)
	return r.executeQuery(indexName, q, 0, 100)
}

/*
* match:

	{
		"query": "apple",
		"fields": [
			"instructions.ingredients",
			"instructions.steps",
			"title"
		],

		"auto_generate_synonyms_phrase_query": true,
		"fuzzy_transpositions": true,
		"boost": 1
	}
*/
func (r *SearchEngine) QueryWithFilter(indexName string, fields []string, text string, filter map[string]string) ([]Hit, error) {
	qt := `{
		"query": {
			"bool":{
				"must":[
					{
						"multi_match":%s
					}
				],
				"filter":{
					"term": %s
				}
			}
		}
	}`
	mm := query_multiMatch{Query: text, Fields: fields, AutoGenerateSynonymsPhraseQuery: true, FuzzyTranspositions: true}
	mmb, err := json.Marshal(mm)
	if err != nil {
		return nil, err
	}
	ftb, err := json.Marshal(filter)
	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf(qt, string(mmb), string(ftb))

	return r.executeQuery(indexName, q, 0, 100)
}

func (r *SearchEngine) QueryFieldById(indexName string, ids, fields []string) ([]Hit, error) {
	qt := `{"docs":%s}`
	type doc struct {
		Index  string   `json:"_index"`
		ID     string   `json:"_id"`
		Source []string `json:"_source"`
	}
	v := []doc{}
	for _, id := range ids {
		v = append(v, doc{Index: indexName, ID: id, Source: fields})
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	q := fmt.Sprintf(qt, string(b))
	req := esapi.MgetRequest{
		Body: strings.NewReader(q),
	}
	fmt.Println("executing query:", q)

	res, err := req.Do(context.Background(), ESClient)

	if err != nil {
		return nil, fmt.Errorf("Query request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil, fmt.Errorf("Query request 404")
	}

	if res.IsError() {
		return nil, fmt.Errorf("Query response: %s", res.String())
	}

	fmt.Println("search result:", res)
	type docRes struct {
		Docs []Hit `json:"docs"`
	}
	var (
		d docRes
	)

	if err := json.NewDecoder(res.Body).Decode(&d); err != nil {
		return nil, fmt.Errorf("Query decode: %w", err)
	}

	return d.Docs, nil
}

func (r *SearchEngine) executeQuery(indexName string, q string, from, size int) ([]Hit, error) {
	req := esapi.SearchRequest{
		Index: []string{indexName},
		Body:  strings.NewReader(q),
		From:  &from,
		Size:  &size,
	}
	fmt.Println("executing query:", q)

	res, err := req.Do(context.Background(), ESClient)

	if err != nil {
		return nil, fmt.Errorf("Query request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil, fmt.Errorf("Query request 404")
	}

	if res.IsError() {
		return nil, fmt.Errorf("Query response: %s", res.String())
	}

	fmt.Println("search result:", res)
	var (
		body queryResponse
	)

	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("Query decode: %w", err)
	}

	return body.Each(), nil
}

func checkInterface(typ reflect.Type, funcname string) bool {
	_, b := typ.MethodByName(funcname)
	return b
}

func (r *queryResponse) Each() []Hit {

	if r.Hits == nil || r.Hits.Hits == nil || len(r.Hits.Hits) == 0 {
		return nil
	}

	return r.Hits.Hits
}
