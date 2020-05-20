package models

type TranslatedMessage struct {
	Location   Location
	Restaurant Restaurant
	Review     Review
	Translated string
}
