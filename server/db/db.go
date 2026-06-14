package db

import "errors"

type Database struct {
	db map[string]string
}

func NewDatabase() (db *Database, err error) {
	keyValueStore := make(map[string]string)
	db = &Database{db: keyValueStore}
	return db, nil
}

func (d *Database) SetKey(key string, value string) error {
	d.db[key] = value
	return nil
}

func (d *Database) GetKey(key string) (string, error) {
	val, exists := d.db[key]
	if !exists {
		return "", errors.New("key not found")
	}
	return val, nil
}

// func (d *Database) deleteKey(key string) error {
// 	_, exists := d.db[key]
// 	if !exists {
// 		return errors.New("key not found")
// 	}
// 	delete(d.db, key)
// 	return nil
// }
