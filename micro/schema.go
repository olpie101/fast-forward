package micro

import (
	"reflect"

	"github.com/invopop/jsonschema"
)

type Schema struct {
	Request  string `json:"request"`
	Response string `json:"response"`
}

func GenerateJsonSchema(reflector jsonschema.Reflector, req interface{}, res interface{}) (*Schema, error) {
	return generateJsonSchema(reflector, req, res)
}

func generateJsonSchema(reflector jsonschema.Reflector, req interface{}, res interface{}) (*Schema, error) {
	reqSchema, err := createSchema(reflector, req)
	if err != nil {
		return nil, err
	}

	resSchema, err := createSchema(reflector, res)
	if err != nil {
		return nil, err
	}

	return &Schema{
		Request:  string(reqSchema),
		Response: string(resSchema),
	}, nil
}

func createSchema(reflector jsonschema.Reflector, v interface{}) ([]byte, error) {
	schema := reflector.Reflect(v)
	schema.Title = extractName(v)
	schemaJson, err := schema.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return schemaJson, nil
}

func extractName(v interface{}) string {
	t := reflect.TypeOf(v)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}
