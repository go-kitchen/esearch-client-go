package util

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

// GetString convert interface to string.
func GetString(v interface{}) string {
	switch result := v.(type) {
	case string:
		return result
	case []byte:
		return string(result)
	default:
		if v != nil {
			return fmt.Sprint(result)
		}
	}
	return ""
}

// IsString check v is string type or not
// only check pure string, do not consider []byte
func IsString(v interface{}) bool {
	switch v.(type) {
	case string:
		return true
	default:
		return false
	}
}

// GetInt convert interface to int.
func GetInt(v interface{}) int {
	switch result := v.(type) {
	case int:
		return result
	case int32:
		return int(result)
	case int64:
		return int(result)
	default:
		if d := GetString(v); d != "" {
			value, _ := strconv.Atoi(d)
			return value
		}
	}
	return 0
}

// GetInt32 convert interface into in32
func GetInt32(v interface{}) int32 {
	switch result := v.(type) {
	case int:
		return int32(result)
	case int32:
		return result
	case int64:
		return int32(result)
	default:
		if d := GetString(v); d != "" {
			value, _ := strconv.ParseInt(d, 10, 32)
			return int32(value)
		}
	}
	return 0
}

// GetInt64 convert interface to int64.
func GetInt64(v interface{}) int64 {
	switch result := v.(type) {
	case int:
		return int64(result)
	case int32:
		return int64(result)
	case int64:
		return result
	case float32:
		return int64(result)
	case float64:
		return int64(result)
	default:
		if d := GetString(v); d != "" {
			value, _ := strconv.ParseInt(d, 10, 64)
			return value
		}
	}
	return 0
}

// GetFloat64 convert interface to float64.
func GetFloat64(v interface{}) float64 {
	switch result := v.(type) {
	case float64:
		return result
	default:
		if d := GetString(v); d != "" {
			value, _ := strconv.ParseFloat(d, 64)
			return value
		}
	}
	return 0
}

// GetBool convert interface to bool.
func GetBool(v interface{}) bool {
	switch result := v.(type) {
	case bool:
		return result
	default:
		if d := GetString(v); d != "" {
			value, _ := strconv.ParseBool(d)
			return value
		}
	}
	return false
}

// GetStringArray convert other types array to string array
func GetStringArray(src []interface{}) []string {
	if src == nil {
		return nil
	}
	result := []string{}
	if len(src) > 0 {
		for _, value := range src {
			result = append(result, fmt.Sprintf("%v", value))
		}
	}
	return result
}

func ConvertInt64ArrayToStringArray(src []int64) []string {
	if src == nil {
		return nil
	}
	var result []string
	if len(src) > 0 {
		for _, value := range src {
			result = append(result, fmt.Sprintf("%d", value))
		}
	}
	return result
}

// FlattenIntArray convert a int array to string and separate by ","
func FlattenIntArray(src []int) string {
	return strings.Trim(strings.Join(strings.Fields(fmt.Sprint(src)), ","), "[]")
}

// FlattenInt32Array convert a int array to string and separate by ","
func FlattenInt32Array(src []int32) string {
	return strings.Trim(strings.Join(strings.Fields(fmt.Sprint(src)), ","), "[]")
}

// FlattenInt64Array convert a int64 array to string and separate by ","
func FlattenInt64Array(src []int64) string {
	return strings.Trim(strings.Join(strings.Fields(fmt.Sprint(src)), ","), "[]")
}

// Flatten flat a string array to joined strings like "a,b,c"
func Flatten(src []string, sep string) string {
	return strings.Join(src, sep)
}

// FlattenForIn flat a string array to joined strings like "'a','b','c'" for sql IN
func FlattenForIn(src []string) string {
	result := ""
	if len(src) == 0 {
		return result
	}
	for _, e := range src {
		result += "'" + e + "',"
	}
	return strings.TrimSuffix(result, ",")
}

// CamelToSnake convert camelCase to snake case, such as xxxYyy => xxx_yyy
func CamelToSnake(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// SnakeToCamel convert snake case to camel
func SnakeToCamel(s string) string {
	if len(s) == 0 {
		return s
	}
	var result string
	words := strings.Split(s, "_")
	for i, word := range words {
		if (i > 0) && len(word) > 0 {
			w := []rune(word)
			w[0] = unicode.ToUpper(w[0])
			result += string(w)
		} else {
			result += word
		}
	}
	return result
}

// Round is round method for f and keep n precision in decimal part
func Round(f float64, n int) float64 {
	n10 := math.Pow10(n)
	return math.Trunc((f+0.5/n10)*n10) / n10
}

func MapToJson(m map[string]interface{}) (jstr string) {
	if m == nil {
		return ""
	}
	b, err := json.Marshal(m)
	if err != nil {
		fmt.Println("MapToJson error:", err)
		return ""
	}
	return string(b)
}

func StructToMap(obj interface{}) map[string]interface{} {
	objValue := reflect.ValueOf(obj)
	if objValue.Kind() != reflect.Struct {
		panic("Input is not a struct")
	}

	objType := objValue.Type()
	resultMap := make(map[string]interface{})

	for i := 0; i < objValue.NumField(); i++ {
		field := objValue.Field(i)
		fieldName := objType.Field(i).Name
		resultMap[fieldName] = field.Interface()
	}

	return resultMap
}
