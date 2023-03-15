package micro

import (
	"reflect"

	"github.com/invopop/jsonschema"
	"github.com/nats-io/nats.go/micro"
)

func GenerateSchema(reflector jsonschema.Reflector, req interface{}, res interface{}) (*micro.Schema, error) {
	reqSchema, err := createSchema(reflector, req)
	if err != nil {
		return nil, err
	}

	resSchema, err := createSchema(reflector, res)
	if err != nil {
		return nil, err
	}

	return &micro.Schema{
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
	return t.Elem().Name()
}
