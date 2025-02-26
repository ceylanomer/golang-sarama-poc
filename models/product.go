package models

// ProductEvent represents a product event in the system
// It contains basic information about a product that can be used
// for both create/update and delete operations
type ProductEvent struct {
	ID   string `json:"id"`   // Unique identifier of the product
	Name string `json:"name"` // Name of the product
}
