package domain

import (
	"fmt"
	"reflect"
	"strings"
)

const (
	TagName = "ghbase"

	AttrColumnFamily = "column_family"
	AttrColumns      = "columns"
	AttrRowKey       = "rowkey"
)

type Schema interface {
	GetNamespace() string
	GetTableName() string
}

type ColumnFamily map[string]string

type HbaseModel[T Schema] struct {
	S T

	IsParsed     bool
	tagToField   map[string]string
	fieldToTag   map[string]string
	columnFamily []string
	cfQualifier  map[string][]string
}

func (h *HbaseModel[T]) GetNamespace() string {
	return h.S.GetNamespace()
}

func (h *HbaseModel[T]) GetTableName() string {
	return h.S.GetTableName()
}

func (h *HbaseModel[T]) GetColumnFamilies() ([]string, error) {
	if h.columnFamily == nil {
		if e := h.ParseSchema(); e != nil {
			return nil, fmt.Errorf("parse schema failed, detail: %v", e.Error())
		}
	}

	return h.columnFamily, nil
}

func (h *HbaseModel[T]) GetColumns(cf string) ([]string, error) {
	if h.cfQualifier == nil {
		if e := h.ParseSchema(); e != nil {
			return nil, fmt.Errorf("parse schema failed, detail: %v", e.Error())
		}
	}

	return h.cfQualifier[cf], nil
}

func (h *HbaseModel[T]) FromMap(rowkey string, mapValues map[string]map[string]string) error {

	if !h.IsParsed {
		if e := h.ParseSchema(); e != nil {
			return e
		}
	}

	var schema reflect.Value

	if reflect.TypeOf(h.S).Kind() == reflect.Ptr {
		schema = reflect.ValueOf(h.S).Elem()
	} else {
		schema = reflect.ValueOf(h.S)
	}

	for _, cf := range h.columnFamily {
		fieldName := h.tagToField[cf]
		fieldValue := schema.FieldByName(fieldName)

		// 将contactsMap赋值给fieldValue
		if fieldValue.CanSet() {
			fieldValue.Set(reflect.ValueOf(mapValues[cf]))
		}
	}

	rowkeyField := schema.FieldByName(h.tagToField[AttrRowKey])
	if rowkeyField.CanSet() {
		rowkeyField.Set(reflect.ValueOf(rowkey))
	}

	return nil
}

func (h *HbaseModel[T]) ToMap() (string, map[string]map[string]string, error) {
	var result = make(map[string]map[string]string)
	var cfs []string
	var err error
	var schema reflect.Value

	if reflect.TypeOf(h.S).Kind() == reflect.Ptr {
		schema = reflect.ValueOf(h.S).Elem()
	} else {
		schema = reflect.ValueOf(h.S)
	}

	if cfs, err = h.GetColumnFamilies(); err != nil {
		return "", nil, err
	}

	for _, cf := range cfs {
		fieldName := h.tagToField[cf]
		fieldValue := schema.FieldByName(fieldName)

		// 将contactsMap赋值给fieldValue
		if fieldValue.CanSet() {
			result[cf] = fieldValue.Interface().(ColumnFamily)
		}
	}

	rowkeyField := schema.FieldByName(h.tagToField[AttrRowKey])
	if rowkeyField.CanSet() {
		return rowkeyField.Interface().(string), result, nil
	}

	return "", result, nil
}

func (h *HbaseModel[T]) ParseSchema() error {
	if h.IsParsed {
		return nil
	}

	var v any
	var rowkeyDefined bool
	var schema = h.S
	var cfs []string
	var cfQualifier = make(map[string][]string)
	var fieldToTag = make(map[string]string)
	var tagToField = make(map[string]string)

	if reflect.TypeOf(schema).Kind() == reflect.Ptr {
		v = reflect.ValueOf(schema).Elem().Interface()
	} else {
		v = reflect.ValueOf(schema).Interface()
	}

	for _, v := range reflect.VisibleFields(reflect.TypeOf(v)) {

		if ghbaseConfig := v.Tag.Get(TagName); ghbaseConfig != "" {
			tags := parseGhbaseTag(ghbaseConfig)

			fieldName := v.Name

			if tags[AttrRowKey] != nil && len(tags) == 1 {
				rowkeyDefined = true
				fieldToTag[fieldName] = AttrRowKey
				tagToField[AttrRowKey] = fieldName
				continue
			}

			if tags[AttrColumnFamily] == nil {
				continue
			} else {
				cf := tags[AttrColumnFamily][0]
				fieldToTag[fieldName] = cf
				tagToField[cf] = fieldName
				cfs = append(cfs, cf)
				if tags[AttrColumns] != nil {
					cfQualifier[cf] = tags[AttrColumns]
				}
			}
		}
	}

	if !rowkeyDefined {
		return fmt.Errorf("rowkey is not defined")
	}

	h.columnFamily = cfs
	h.cfQualifier = cfQualifier
	h.fieldToTag = fieldToTag
	h.tagToField = tagToField
	h.IsParsed = true

	return nil
}

func SnakeCaseToCamelCase(input string) string {
	// 分割字符串
	words := strings.Split(input, "_")

	// 将每个单词的首字母大写
	for i, word := range words {
		words[i] = strings.Title(word)
	}

	// 连接单词
	return strings.Join(words, "")
}

func parseGhbaseTag(tag string) map[string][]string {
	var re = map[string][]string{}
	// split with ;
	properties := strings.Split(tag, ";")
	// split with :
	for _, v := range properties {
		// split with :
		kvs := strings.Split(v, ":")
		// kvs[0] is property_name
		property_name := kvs[0]
		// kvs[1] is property_values
		// split with ,
		if len(kvs) == 1 {
			re[property_name] = []string{}
			continue
		}

		options := strings.Split(kvs[1], ",")
		re[property_name] = options
	}

	return re
}
