package test

import (
	"log"
	"testing"

	"github.com/go-kitchen/esearch-client-go/client"
)

var (
	s         *client.SearchEngine
	indexName string
	gotId     string
)

func init() {
	e := client.InitESClient([]string{"http://127.0.0.1:9200"})
	if e != nil {
		log.Fatal("Initing Client Error: ", e)
	}
	s = &client.SearchEngine{}
	indexName = "recipe_data"
}

func TestSearchEngine(t *testing.T) {

	//create an index
	err := s.Index(indexName)
	if err != nil {
		t.Errorf("Creating Index error:%v", err)
	}

	//add a document
	recipe := &CardRender{
		Title:     "test add doc: apple recipe",
		CreatorID: 1,
		ID:        "Z9VukIoBTBIIioll_YjA",
		Serves:    8,
		Img:       "https://cookage-recipe-src.s3.amazonaws.com/user-data/fisher/recipe_images/cd898b9c647594e14097867e299b801b.jpg",
		Instructions: []InstructRender{
			{
				GroupTitle: "",
				Ingredient: []string{
					"all-purpose flour",
					"butter",
				},
				Steps: []string{
					"1. Place in greased 7 1/2 x 13 baking dish, five or six sliced apples.",
					"2. (Peeled or unpeeled).",
					"3. Mix the following together with a fork until crumbly: 1 cup sifted all-purpose flour; 1/2 to 1 cup sugar (depending on tartness of apples); 1 tsp baking powder; 1/4 tsp of salt; 1 unbeaten egg.",
					"4. Sprinkle over apples.",
				},
			},
		},
	}

	gotId, err = s.AddDoc(indexName, recipe)
	if err != nil {
		t.Errorf("AddDoc error: %v", err)
	}
	if gotId == "" {
		t.Errorf("No auto assign ID")
	}

	//query by id
	hit, err := s.GetOne(indexName, gotId)
	if err != nil {
		t.Errorf("No document:%v", err)
	}
	card, err := Hit2Card(hit)
	if err != nil {
		t.Errorf("Error in converting:%v", err)
	}
	if card == nil {
		t.Errorf("Error in converting card:%vs", card)
	}

	//filter by fields
	results, err := s.FilterQuery(indexName, map[string]interface{}{"user_id": 1})
	if err != nil {
		t.Errorf("Error in filter query:%v", err)
	}
	if len(results) == 0 {
		t.Errorf("Error in filtering:%v", results)
	}

	//update Doc
	recipe.CreatorID = 2
	var list []client.SearchEngine_Doc
	list = append(list, recipe)
	err = s.BulkUpdate(indexName, list)
	if err != nil {
		t.Errorf("Error in updating: %v", err)
	}

	//delete by id
	err = s.BulkDelete(indexName, []string{gotId})
	if err != nil {
		t.Errorf("Error in Deletion: %v", err)
	}
}
