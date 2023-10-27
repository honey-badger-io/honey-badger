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

	t.Run("should delete value by key and all tag entries", func(t *testing.T) {
		const key = "test-key-tag"
		db.Set(key, []byte{1}, 0, []string{"delete-tag"})

		if err := db.DeleteByKey(key); err != nil {
			panic(err)
		}

		_, hit, err := db.Get(key)
		_, tagItemFound, _ := db.Get("delete-tag" + TagDelimiter + key)

		assert.False(t, hit)
		assert.Nil(t, err)
		assert.False(t, tagItemFound, "tag item should NOT be found")
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

		err := db.DeleteByPrefix("deleteprefix")

		_, foundKey1, _ := db.Get(key1)
		_, foundKey2, _ := db.Get(key2)

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.False(t, foundKey1, "key1 should NOT be found")
		assert.False(t, foundKey2, "key2 should NOT be found")
	})

	t.Run("should delete data by prefix with all tag items", func(t *testing.T) {
		var key = "deleteprefix-tag-test-1"

		db.Set(key, make([]byte, 1), 0, []string{"tag"})

		err := db.DeleteByPrefix("deleteprefix")

		_, foundTagItem, _ := db.Get("tag" + TagDelimiter + key)

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.False(t, foundTagItem, "tag item should NOT be found")
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

	t.Run("should delete entries by tag with all tag entries", func(t *testing.T) {
		db.Set("TagItem-1", []byte{1}, 0, []string{"FirstTag"})
		db.Set("TagItem-2", []byte{2}, 0, []string{"FirstTag", "SecondTag"})

		err := db.DeleteByTag("FirstTag")
		_, found1, _ := db.Get("TagItem-1")
		_, found2, _ := db.Get("TagItem-2")
		_, foundTagEntry, _ := db.Get("SecondTag_tag_TagItem-2")

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.False(t, found1, "'TagItem-1' not removed")
		assert.False(t, found2, "'TagItem-2' not removed")
		assert.False(t, foundTagEntry, "'SecondTag_tag_TagItem-2' not removed")
	})
}

func TestWriter(t *testing.T) {
	db := getDb()

	t.Run("should set data with writer", func(t *testing.T) {
		w := db.NewWriter()
		key := "test-writer-key"

		err := w.Write(&pb.DataItem{
			Key:  key,
			Data: make([]byte, 1),
		})
		errCommit := w.Commit()
		w.Close()

		_, hit, _ := db.Get(key)

		assert.Nil(t, err, fmt.Sprintf("%v", err))
		assert.Nil(t, errCommit, fmt.Sprintf("%v", errCommit))
		assert.True(t, hit)
	})

	t.Run("should set data with writer and tags", func(t *testing.T) {
		w := db.NewWriter()
		key := "test-writer-key-with-tag"
		tag := "test-writer-tag"

		w.Write(&pb.DataItem{
			Key:  key,
			Data: make([]byte, 1),
			Tags: []string{tag},
		})
		w.Commit()
		w.Close()

		tags, _ := db.GetTags(key, nil)

		assert.Equal(t, tag, tags[0])
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
