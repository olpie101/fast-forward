package micro

import (
	"fmt"
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
			extraTagVals, _ := f.Tag.Lookup("jsonschema")
			nf := reflect.StructField{
				Name:      strings.ReplaceAll(jsonschema.ToSnakeCase(f.Name), "-", "_"),
				PkgPath:   "",
				Type:      reflect.TypeOf(""),
				Tag:       reflect.StructTag(fmt.Sprintf(`%s jsonschema:"%s,format=uuid"`, f.Tag, extraTagVals)),
				Offset:    0,
				Index:     []int{},
				Anonymous: false,
			}
			out = append(out, nf)
		}
	}
	return out
}
