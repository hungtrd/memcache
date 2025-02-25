package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Item struct {
	ID    string `bson:"_id"`
	Name  string `bson:"name"`
	Value string `bson:"value"`
}

type Cache struct {
	mu             sync.Mutex
	items          []Item
	client         *mongo.Client
	dbName         string
	collectionName string
}

func NewCache(dbName, collectionName string) (*Cache, error) {
	client, err := mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI("mongodb://localhost:27017"),
	)
	if err != nil {
		return nil, err
	}

	return &Cache{
		mu:             sync.Mutex{},
		items:          []Item{},
		client:         client,
		dbName:         dbName,
		collectionName: collectionName,
	}, nil
}

func (c *Cache) Load() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	collection := c.client.Database(c.dbName).Collection(c.collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var items []Item
	if err = cursor.All(ctx, &items); err != nil {
		return err
	}
	c.items = items

	return nil
}

func (c *Cache) getFromDB(id string) (Item, error) {
	collection := c.client.Database(c.dbName).Collection(c.collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var item Item
	err := collection.FindOne(ctx, bson.M{"_id": id}).Decode(&item)
	if err != nil {
		return Item{}, err
	}
	return item, nil
}

func (c *Cache) Set(item Item) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = append(c.items, item)
}

func (c *Cache) Get(id string) (Item, error) {
	c.mu.Lock()
	for _, item := range c.items {
		if item.ID == id {
			return item, nil
		}
	}
	c.mu.Unlock()

	item, err := c.getFromDB(id)
	if err != nil {
		return Item{}, err
	}
	fmt.Println("Setting new item in cache: ", item.ID)
	c.Set(item)

	return item, nil
}

func initItems() error {
	client, err := mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI("mongodb://localhost:27017"),
	)
	if err != nil {
		return err
	}

	collection := client.Database("testDb").Collection("products")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	items := []interface{}{
		Item{ID: "1", Name: "Item1", Value: "Value1"},
		Item{ID: "2", Name: "Item2", Value: "Value2"},
		Item{ID: "3", Name: "Item3", Value: "Value3"},
	}

	_, err = collection.InsertMany(ctx, items)

	return err
}

func insertItemsToDB() error {
	client, err := mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI("mongodb://localhost:27017"),
	)
	if err != nil {
		return err
	}

	collection := client.Database("testDb").Collection("products")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	items := []interface{}{
		Item{ID: "4", Name: "Item4", Value: "Value4"},
		Item{ID: "5", Name: "Item5", Value: "Value5"},
	}

	_, err = collection.InsertMany(ctx, items)

	return err
}

func main() {
	memCache, err := NewCache("testDb", "products")
	if err != nil {
		log.Fatal(err.Error())
	}
	memCache.Load()
	fmt.Println("Loaded all items from DB")

	if err := insertItemsToDB(); err != nil {
		fmt.Println("Error inserting item to DB: ", err)
	}

	fmt.Println("Sleeping..")
	time.Sleep(5 * time.Second)

	item, err := memCache.Get("4")
	if err != nil {
		log.Println("error: ", err)
	}
	fmt.Println(item)
}
