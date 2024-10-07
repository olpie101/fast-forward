package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/gobeam/stringy"
	"github.com/invopop/jsonschema"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	ffmicro "github.com/olpie101/fast-forward/micro"
)

func main() {
	serviceName := flag.String("service-name", "CardService", "service name to generate schema for")
	templatePath := flag.String("template", "", "template file")
	outPath := flag.String("output", "card.gen.ts", "output file location")
	importDefinitionsPath := flag.String("import-definitions-path", "", "import go definitions from")
	language := flag.String("lang", "ts", "language to generate")
	casing := flag.String("casing", "snake", "method casing")
	flag.Parse()

	templateName := filepath.Base(*templatePath)

	var err error
	if *language == "python" {
		err = executePython(*serviceName, *templatePath, templateName, *importDefinitionsPath, *language, *casing, *outPath)
	} else {
		err = execute(*serviceName, *templatePath, templateName, *importDefinitionsPath, *language, *casing, *outPath)
	}

	if err != nil {
		log.Fatal(err)
	}
}

func execute(name string, templatePath, templateName, importDefs, lang, casing, outPath string) error {
	fmt.Println(">>>>>", os.Getenv("NATS_URL"))
	fmt.Println(">>>>>", os.Getenv("NATS_CREDS"))
	nc, err := nats.Connect(
		os.Getenv("NATS_URL"),
		nats.UserCredentials(os.Getenv("NATS_CREDS")),
	)
	if err != nil {
		return err
	}
	sub, err := micro.ControlSubject(micro.InfoVerb, name, "")
	if err != nil {
		return err
	}
	m, err := nc.Request(sub, nil, time.Second)
	if err != nil {
		return err
	}
	var resp micro.Info
	err = json.Unmarshal(m.Data, &resp)
	if err != nil {
		return err
	}

	packageParts := strings.Split(importDefs, "/")
	importPackage := ""
	if len(packageParts) > 0 {
		importPackage = packageParts[len(packageParts)-1]
		fmt.Println("import pac", importPackage)
	}

	data := TemplateData{
		Name:           resp.Name,
		Endpoints:      []EndpointConfig{},
		ImportPackage:  importDefs,
		PackagePrefix:  importPackage,
	}
	fmt.Println("ID:", resp.ID, resp.ServiceIdentity)

	wildcardCountSet := false

	fmt.Println("templating", templatePath, templateName)
	t, err := template.New("service").ParseFiles(templatePath)
	if err != nil {
		return err
	}

	for _, ep := range resp.Endpoints {
		var cfg EndpointConfig
		cfg.Name = convertToCase(ep.Name, casing)
		cfg.Subject = ep.Subject
		wc := strings.Count(ep.Subject, "*")
		if !wildcardCountSet && wc > 0 {
			wildcardCountSet = true
			data.SubjectParamsCount = wc
		}
		if wildcardCountSet && wc != data.SubjectParamsCount {
			return errors.New("cannot generate client for endpoints with varying wildcard counts")
		}

		var schema ffmicro.Schema
		err := json.Unmarshal([]byte(ep.Metadata["schema"]), &schema)
		if err != nil {
			return err
		}

		reqSchema, err := unmarshalSchema(schema.Request)
		if err != nil {
			return err
		}

		requestName := extractName(reqSchema)
		cfg.Schema.Request = requestName

		respSchema, err := unmarshalSchema(schema.Response)
		if err != nil {
			return err
		}
		responseName := extractName(respSchema)
		cfg.Schema.Response = responseName

		fmt.Println(">>>0", schema.Request)
		fmt.Println(">>>1", schema.Response)
		iData := []byte{}
		if lang == "ts" {
			iData, err = generateReqRespInterfaces(reqSchema, respSchema)
			if err != nil {
				return err
			}
		}
		fmt.Println(">>>1", len(iData))
		cfg.InterfaceData = string(iData)
		data.Endpoints = append(data.Endpoints, cfg)
	}

	if data.SubjectParamsCount > 0 {
		def := strings.TrimSuffix(strings.Repeat("string,", data.SubjectParamsCount), ",")
		data.SubjectParams = fmt.Sprintf("[%s]", def)
	}

	var w bytes.Buffer
	err = t.ExecuteTemplate(&w, templateName, data)
	if err != nil {
		return err
	}

	fmt.Println("writing")
	return os.WriteFile(outPath, w.Bytes(), 0644)
}

func executePython(name string, templatePath, templateName, importDefs, lang, casing, outPath string) error {
	nc, err := nats.Connect(
		os.Getenv("NATS_URL"),
		nats.UserCredentials(os.Getenv("NATS_CREDS")),
	)
	if err != nil {
		return err
	}
	sub, err := micro.ControlSubject(micro.InfoVerb, name, "")
	if err != nil {
		return err
	}
	m, err := nc.Request(sub, nil, time.Second)
	if err != nil {
		return err
	}
	var resp micro.Info
	err = json.Unmarshal(m.Data, &resp)
	if err != nil {
		return err
	}

	packageParts := strings.Split(importDefs, "/")
	importPackage := ""
	if len(packageParts) > 0 {
		importPackage = packageParts[len(packageParts)-1]
	}

	data := TemplateData{
		Name:           resp.Name,
		Endpoints:      []EndpointConfig{},
		ImportPackage:  importDefs,
		PackagePrefix:  importPackage,
	}

	wildcardCountSet := false

	t, err := template.New("service").ParseFiles(templatePath)
	if err != nil {
		return err
	}

	for _, ep := range resp.Endpoints {
		var cfg EndpointConfig
		cfg.Name = convertToCase(ep.Name, casing)
		cfg.Subject = ep.Subject
		wc := strings.Count(ep.Subject, "*")
		if !wildcardCountSet && wc > 0 {
			wildcardCountSet = true
			data.SubjectParamsCount = wc
		}
		if wildcardCountSet && wc != data.SubjectParamsCount {
			return errors.New("cannot generate client for endpoints with varying wildcard counts")
		}

		var schema ffmicro.Schema
		err := json.Unmarshal([]byte(ep.Metadata["schema"]), &schema)
		if err != nil {
			return err
		}

		reqSchema, err := unmarshalSchema(schema.Request)
		if err != nil {
			return err
		}

		respSchema, err := unmarshalSchema(schema.Response)
		if err != nil {
			return err
		}

		cfg.Schema.Request = extractName(reqSchema)
		cfg.Schema.Response = extractName(respSchema)

		if lang == "python" {
			cfg.Schema.RequestType = generatePythonTypeDefinition(cfg.Schema.Request, reqSchema)
			cfg.Schema.ResponseType = generatePythonTypeDefinition(cfg.Schema.Response, respSchema)
		}

		data.Endpoints = append(data.Endpoints, cfg)
	}

	if data.SubjectParamsCount > 0 {
		def := strings.TrimSuffix(strings.Repeat("str,", data.SubjectParamsCount), ",")
		data.SubjectParams = fmt.Sprintf("List[%s]", def)
	}

	var w bytes.Buffer
	err = t.ExecuteTemplate(&w, templateName, data)
	if err != nil {
		return err
	}

	return os.WriteFile(outPath, w.Bytes(), 0644)
}

func convertToCase(s, casing string) string {
	var out stringy.StringManipulation
	switch casing {
	case "camel":
		out = stringy.New(s).CamelCase()
	case "pascal":
		out = stringy.New(s).PascalCase()
	default:
		out = stringy.New(s).SnakeCase()
	}
	return out.Get()
}

type TemplateData struct {
	Name               string
	SubjectParams      string
	SubjectParamsCount int
	ImportPackage      string
	PackagePrefix      string
	Endpoints          []EndpointConfig
}

type EndpointConfig struct {
	Name          string
	Subject       string
	Schema        RequestResponseSchema
	ImportPackage string
	InterfaceData string
}

type RequestResponseSchema struct {
	Request      string
	Response     string
	RequestType  string
	ResponseType string
}

func unmarshalSchema(def string) (*jsonschema.Schema, error) {
	var schema jsonschema.Schema
	err := json.NewDecoder(bytes.NewReader([]byte(def))).Decode(&schema)
	if err != nil {
		return nil, err
	}
	return &schema, nil
}

func extractName(schema *jsonschema.Schema) string {
	return schema.Title
}

func generateReqRespInterfaces(schemas ...*jsonschema.Schema) ([]byte, error) {
	var tw bytes.Buffer
	for _, s := range schemas {
		cmd := exec.Command("json2ts", "--bannerComment", "")
		b, err := s.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("cannot decode: %w", err)
		}
		r := bytes.NewReader(b)
		cmd.Stdin = r
		var w bytes.Buffer
		cmd.Stdout = &w
		err = cmd.Run()
		if err != nil {
			return nil, fmt.Errorf("unable to execute: %w", err)
		}
		err = cmd.Err
		if err != nil {
			return nil, fmt.Errorf("command err: %w", err)
		}
		_, err = tw.Write(w.Bytes())
		if err != nil {
			return nil, err
		}
	}
	return tw.Bytes(), nil
}

func generatePythonTypeDefinition(name string, schema *jsonschema.Schema) string {
	var def strings.Builder
	def.WriteString(fmt.Sprintf("class %s(TypedDict):\n", name))
	
	if schema.Properties != nil {
		for pair := schema.Properties.Oldest(); pair != nil; pair = pair.Next() {
			key := pair.Key
			value := pair.Value
			typeName := getPythonType(value)
			def.WriteString(fmt.Sprintf("    %s: %s\n", key, typeName))
		}
	}
	
	if schema.Properties == nil || schema.Properties.Len() == 0 {
		def.WriteString("    pass\n")
	}
	
	def.WriteString("\n")
	
	return def.String()
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