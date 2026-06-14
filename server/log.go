package server

import "encoding/json"

func ToLog(req ClientCommand) (string, error) {
	log := Log{key: req.key, value: req.value}
	b, err := json.Marshal(log)
	if err != nil {
		return "", err
	}

	return string(b), err
}

func ReadLog(log string) (*Log, error) {
	var l Log
	err := json.Unmarshal([]byte(log), &l)
	if err != nil {
		return nil, err
	}

	return &l, nil
}
