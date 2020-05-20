package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Review struct {
	ID            primitive.ObjectID `bson:"_id"`
	Id            string             `bson:"id"`
	Lang          string             `bson:"lang"`
	LocationId    string             `bson:"location_id"`
	PublishedDate string             `bson:"published_date"`
	Text          string             `bson:"text"`
	Rating        string             `bson:"rating"`
	Vader         float64            `bson:"vader"`
	Wordnet       float64            `bson:"wordnet"`
	RestaurantID  primitive.ObjectID `bson:"restaurant_ObjectId"`
	Subratings    []Subrating        `bson:"subratings"`
	CreatedAt     time.Time          `bson:"created_at"`
}

type Subrating struct {
	Name  string `bson:"name"`
	Value string `bson:"value"`
}
