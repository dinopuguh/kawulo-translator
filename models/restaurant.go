package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Restaurant struct {
	ID                 primitive.ObjectID `bson:"_id"`
	LocationId         string             `bson:"location_id"`
	Name               string             `bson:"name"`
	Latitude           string             `bson:"latitude"`
	Longitude          string             `bson:"longitude"`
	NumReviews         string             `bson:"num_reviews"`
	Photo              Photo              `bson:"photo"`
	Rating             string             `bson:"rating"`
	PriceLevel         string             `bson:"price_level"`
	Price              string             `bson:"price"`
	Address            string             `bson:"address"`
	Phone              string             `bson:"phone"`
	Website            string             `bson:"website"`
	RawRanking         string             `bson:"raw_ranking"`
	RankingGeo         string             `bson:"ranking_geo"`
	RankingPosition    string             `bson:"ranking_position"`
	RankingDenominator string             `bson:"ranking_denominator"`
	RankingCategory    string             `bson:"ranking_category"`
	Ranking            string             `bson:"ranking"`
	SubCategory        []SubCategory      `bson:"subcategory"`
	LocationID         string             `bson:"locationID"`
	LocationObjectID   primitive.ObjectID `bson:"location_ObjectId"`
	CreatedAt          time.Time          `bson:"created_at"`
}

type Photo struct {
	Images Images `bson:"images"`
}

type Images struct {
	Thumbnail Image `bson:"thumbnail"`
	Original  Image `bson:"original"`
	Medium    Image `bson:"medium"`
	Large     Image `bson:"large"`
}

type Image struct {
	Width  string `bson:"width"`
	Url    string `bson:"url"`
	Height string `bson:"height"`
}

type SubCategory struct {
	Key  string `bson:"key"`
	Name string `bson:"name"`
}
