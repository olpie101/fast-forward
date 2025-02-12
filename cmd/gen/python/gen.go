package python

import (
	"bytes"
	"fmt"

	"github.com/invopop/jsonschema"
)



func GenerateTypes(reqName string,reqSchema *jsonschema.Schema,respName string , respSchema *jsonschema.Schema )([]byte,error){
	var def bytes.Buffer
    d,err:= generatePythonTypeDefinition(reqName,reqSchema)
	if err!=nil{
		return nil,err
	}
	_, err = def.Write(append(d, []byte("\n")...))
	
	if err != nil {
		return nil, err
	}
	
	    d,err = generatePythonTypeDefinition(respName,respSchema)
	if err!=nil{
		return nil,err
	}
	_, err = def.Write(append(d, []byte("\n")...))
		if err!=nil{
		return nil,err
	}
	return def.Bytes(),nil

}

func generatePythonTypeDefinition(name string, schema *jsonschema.Schema) ([]byte,error) {
	var def bytes.Buffer
	_,err:=def.WriteString(fmt.Sprintf("class %s(TypedDict):\n", name))
		if err!=nil{
		return nil,err
	}
	if schema.Properties != nil {
		for pair := schema.Properties.Oldest(); pair != nil; pair = pair.Next() {
			key := pair.Key
			value := pair.Value
			typeName := getPythonType(value)
			_,err =def.WriteString(fmt.Sprintf("    %s: %s\n", key, typeName))
				if err!=nil{
				return nil,err
			}
		}
	}
	
	if schema.Properties == nil || schema.Properties.Len() == 0 {
		_,err:= def.WriteString("    pass\n")
		if err!=nil{
			return nil,err
		}
	}
	
	_,err = def.WriteString("\n")
		if err!=nil{
			return nil,err
		}
	return def.Bytes() , nil
	
}

func getPythonType(prop *jsonschema.Schema) string {
	switch prop.Type {
	case "string":
		return "str"
	case "integer":
		return "int"
	case "number":
		return "float"
	case "boolean":
		return "bool"
	case "array":
		if prop.Items != nil {
			itemType := getPythonType(prop.Items)
			return fmt.Sprintf("List[%s]", itemType)
		}
		return "List[Any]"
	case "object":
		return "Dict[str, Any]"
	default:
		return "Any"
	}
}