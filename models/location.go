package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Location struct {
	ID         primitive.ObjectID `bson:"_id"`
	Name       string             `bson:"name"`
	LocationId string             `bson:"location_id"`
	Latitude   string             `bson:"latitude"`
	Longitude  string             `bson:"longitude"`
	CreatedAt  time.Time          `bson:"created_at"`
}
