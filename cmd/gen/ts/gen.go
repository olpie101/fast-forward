package ts

import (
	"bytes"
	"fmt"
	"os/exec"

	"github.com/invopop/jsonschema"
)

func GenerateTypes(schemas ...*jsonschema.Schema) ([]byte, error) {
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
