package example

import (
	"encoding/json"
	"fmt"

	"github.com/go-kitchen/esearch-client-go/client"
	"github.com/go-kitchen/esearch-client-go/util"
)

type CardRender struct {
	ID string `json:"id"`
	// MGID      primitive.ObjectID `bson:"id" json:"mgid"`
	CreatorID int32 `bson:"user_id" json:"user_id"`

	/*------ visible information ------*/
	Title        string           `bson:"title" json:"title"`
	Subheader    string           `bson:"subheader" json:"subheader"`
	Story        string           `json:"intro" bson:"intro"`
	Sign         string           `json:"sign" bson:"sign"`
	Serves       int8             `json:"serves" bson:"serves"`
	Img          string           `bson:"img" json:"img"`
	CoverImg     string           `bson:"coverImg" json:"coverImg"`
	Instructions []InstructRender `json:"instructions"`
	Meal         string           `json:"meal" bson:"meal"`

	// Ingredients  []string         `json:"ingredients" bson:"ingredients"`
	// Steps        []string         `json:"steps" bson:"steps"`
	// Instructions []Instruction `json:"instructions" bson:"instructions"`

	/*------ visible information ------*/

	Lang     string   `json:"lang" bson:"lang"`
	Cal      string   `json:"cal" bson:"cal"`
	Labels   []string `json:"labels" bson:"labels"`
	Category string   `json:"category" bson:"category"`
	Template string   `json:"template"`
}

type InstructRender struct {
	GroupTitle string   `json:"group_title"`
	Ingredient []string `json:"ingredients"`
	Steps      []string `json:"steps"`
}

func (r CardRender) ToJSON() string {
	data, err := json.Marshal(r)
	if err != nil {
		return ""
	}
	m := map[string]interface{}{}
	json.Unmarshal(data, &m)
	delete(m, "id")
	data, _ = json.Marshal(m)

	return string(data)
}

func (r CardRender) SetID(id string) {
	r.ID = id
}

func (r CardRender) GetID() string {
	return r.ID
}

func Hits2Cards(hits []client.Hit) (list []CardRender, err error) {
	if len(hits) == 0 {
		return nil, fmt.Errorf("Empty results")
	}

	for _, hit := range hits {
		var c CardRender
		if err := json.Unmarshal(hit.Source, &c); err == nil {
			c.ID = hit.ID
			list = append(list, c)
		}
	}
	return
}

func (r CardRender) FieldsToMap() map[string]interface{} {
	m := util.StructToMap(r)
	return m
}

func Hit2Card(h *client.Hit) (c *CardRender, err error) {
	if h == nil {
		return nil, fmt.Errorf("Empty hit")
	}

	if err = json.Unmarshal(h.Source, &c); err == nil {
		c.ID = h.ID
	}

	return
}
