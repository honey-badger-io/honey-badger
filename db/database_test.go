package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/honey-badger-io/honey-badger/config"
	"github.com/honey-badger-io/honey-badger/pb"
	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	db := getDb()

	t.Run("should return Hit=False if key is not found", func(t *testing.T) {
		_, hit, err := db.Get("test-key1")
		if err != nil {
			panic(err)
		}

		assert.False(t, hit)
	})

	t.Run("should return data if key is found", func(t *testing.T) {
		const key = "test-key2"
		db.Set(key, []byte{1, 3, 4}, 0, make([]string, 0))

		data, hit, err := db.Get(key)
		if err != nil {
			panic(err)
		}

		assert.True(t, hit)
		assert.NotEmpty(t, data)
	})
}

func TestSet(t *testing.T) {
	db := getDb()

	t.Run("should set key", func(t *testing.T) {
		const key = "set-test-key"
		var data = []byte{1, 2, 3}

		if err := db.Set(key, data, 0, make([]string, 0)); err != nil {
			panic(err)
		}

		dataRes, _, _ := db.Get(key)

		assert.EqualValues(t, data, dataRes)
	})

	t.Run("should set key with tags", func(t *testing.T) {
		const key = "product-1"
		var data = []byte{1, 2, 3}
		var tags = []string{"products", "test"}

		if err := db.Set(key, data, 0, tags); err != nil {
			panic(err)
		}

		dataRes, _, _ := db.Get(key)

		assert.EqualValues(t, data, dataRes)
	})
}

func TestDeleteByKey(t *testing.T) {
	db := getDb()

	t.Run("should delete value by key", func(t *testing.T) {
		const key = "test-key2"
		db.Set(key, []byte{1, 2, 3}, 0, make([]string, 0))

		if err := db.DeleteByKey(key); err != nil {
			panic(err)
		}

		_, hit, err := db.Get(key)

		assert.False(t, hit)
		assert.Nil(t, err)
	})
}

func TestDeleteByPrefix(t *testing.T) {
	db := getDb()

	t.Run("should delete data by prefix", func(t *testing.T) {
		var (
			data1 = []byte{1, 2, 3}
			data2 = []byte{4, 5, 6}
		)
		var (
			key1 = "deleteprefix-test-1"
			key2 = "deleteprefix-test-2"
		)

		db.Set(key1, data1, 0, make([]string, 0))
		db.Set(key2, data2, 0, make([]string, 0))

		if err := db.DeleteByPrefix("deleteprefix"); err != nil {
			panic(err)
		}

		//res, _ := db.GetByPrefix(context.Background(), "deleteprefix")
		//assert.Empty(t, res)
	})
}

func TestReadDataByPrefix(t *testing.T) {
	db := getDb()

	t.Run("should data by prefix", func(t *testing.T) {
		const DataLen = 3
		resultData := make(map[string][]byte)
		writer := db.NewWriter()
		defer writer.Close()

		for i := 0; i < DataLen; i++ {
			writer.Write(&pb.DataItem{
				Key:  fmt.Sprintf("stream-%d", i+1),
				Data: make([]byte, 1),
			})
		}
		writer.Commit()

		err := db.ReadDataByPrefix(context.TODO(), "stream-", func(item *pb.DataItem) error {
			resultData[item.Key] = item.Data
			return nil
		})

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.Equal(t, DataLen, len(resultData))
	})
}

func TestReadDataByTag(t *testing.T) {
	db := getDb()

	t.Run("should data by tag", func(t *testing.T) {
		resultData := make(map[string][]byte)

		db.Set("item-1", []byte{1}, 0, []string{"products"})
		db.Set("item-2", []byte{2}, 0, []string{"products"})

		err := db.ReadDataByTag(context.TODO(), "products", func(item *pb.DataItem) error {
			resultData[item.Key] = item.Data
			return nil
		})

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.Equal(t, 2, len(resultData))
		assert.Equal(t, []byte{1}, resultData["item-1"])
		assert.Equal(t, []byte{2}, resultData["item-2"])
	})
}

func TestDeleteByTag(t *testing.T) {
	db := getDb()

	t.Run("should delete entries by tag", func(t *testing.T) {
		db.Set("TextDeleteByTag-1", []byte{1}, 0, []string{"TextDeleteByTag"})
		db.Set("TextDeleteByTag-2", []byte{2}, 0, []string{"TextDeleteByTag"})

		err := db.DeleteByTag("TextDeleteByTag")
		_, found1, _ := db.Get("TextDeleteByTag-1")
		_, found2, _ := db.Get("TextDeleteByTag-2")

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.False(t, found1, "'TextDeleteByTag-1' not removed")
		assert.False(t, found2, "'TextDeleteByTag-2' not removed")
	})
}

func getDb() *Database {
	ctx := CreateCtx(config.BadgerConfig{})

	db, err := ctx.CreateDb("test", true)
	if err != nil {
		panic(err)

	}

	return db
}
