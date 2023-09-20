package databeat

import (
	"encoding/json"
	"fmt"
)

// Props is a sugar type to make it easier to track props
type Props map[string]interface{}

var _ ToEventProps = Props{}

func (p Props) ToEventProps() (map[string]string, map[string]float64, map[string]interface{}) {
	strProps := map[string]string{}
	numProps := map[string]float64{}
	etcProps := map[string]interface{}{}

	for k, v := range (map[string]interface{})(p) {
		switch t := v.(type) {
		case string:
			strProps[k] = t
		case fmt.Stringer:
			strProps[k] = t.String()
		case []byte:
			strProps[k] = string(t)
		case int:
			numProps[k] = float64(t)
		case int16:
			numProps[k] = float64(t)
		case int32:
			numProps[k] = float64(t)
		case int64:
			numProps[k] = float64(t)
		case uint:
			numProps[k] = float64(t)
		case uint16:
			numProps[k] = float64(t)
		case uint32:
			numProps[k] = float64(t)
		case uint64:
			numProps[k] = float64(t)
		case float32:
			numProps[k] = float64(t)
		case float64:
			numProps[k] = t
		case bool:
			strProps[k] = fmt.Sprintf("%v", t)
		default:
			etcProps[k] = v
		}
	}

	return strProps, numProps, etcProps
}

type ToEventProps interface {
	ToEventProps() (map[string]string, map[string]float64, map[string]interface{})
}

func StructToProps(v any) (map[string]string, map[string]float64, map[string]interface{}, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, nil, nil, err
	}

	var values Props
	err = json.Unmarshal(data, &values)
	if err != nil {
		return nil, nil, nil, err
	}

	strProps, numProps, etcProps := values.ToEventProps()
	return strProps, numProps, etcProps, nil
}
