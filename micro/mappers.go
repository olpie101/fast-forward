package micro

import (
	"reflect"
	"strings"

	"github.com/google/uuid"
	"github.com/invopop/jsonschema"
)

func UUIDMapper(t reflect.Type) []reflect.StructField {
	var out []reflect.StructField
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		isUUID := f.Type == reflect.TypeOf(uuid.UUID{})
		if isUUID {
			nf := reflect.StructField{
				Name:      strings.ReplaceAll(jsonschema.ToSnakeCase(f.Name), "-", "_"),
				PkgPath:   "",
				Type:      reflect.TypeOf(""),
				Tag:       `jsonschema:"format=uuid"`,
				Anonymous: false,
			}
			out = append(out, nf)
		}
	}
	return out
}
