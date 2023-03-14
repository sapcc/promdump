package model

import (
	"fmt"
	"strings"

	"github.com/prometheus/common/model"
)

func ParquetSchemaFor(data map[string]any) (string, error) {
	fields := make([]string, 0)
	for key, val := range data {
		parquetType, err := parquetTypeFor(val)
		if err != nil {
			return "", err
		}
		field := fmt.Sprintf("{\"Tag\": \"name=%s, %s\"}", key, parquetType)
		fields = append(fields, field)
	}
	joinedFields := strings.Join(fields, ",")
	return fmt.Sprintf("{\"Tag\": \"name=data\",\"Fields\": [%s]}", joinedFields), nil
}

func parquetTypeFor(val any) (string, error) {
	switch val.(type) {
	case string:
		return "type=BYTE_ARRAY, convertedtype=UTF8, encoding=DELTA_BYTE_ARRAY, repetitiontype=OPTIONAL", nil
	case model.LabelValue:
		return "type=BYTE_ARRAY, convertedtype=UTF8, encoding=DELTA_BYTE_ARRAY, repetitiontype=OPTIONAL", nil
	case int64:
		return "type=INT64, encoding=DELTA_BINARY_PACKED, repetitiontype=OPTIONAL", nil
	case float64:
		return "type=DOUBLE, repetitiontype=OPTIONAL", nil
	}
	return "", fmt.Errorf("unknown type %T for parquet schema generation", val)
}
