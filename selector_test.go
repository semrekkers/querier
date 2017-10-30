package sugar

import "testing"

type fieldsModel struct {
	ID        int
	Username  string
	FirstName string
	LastName  string
	Age       int

	Complex struct {
		OtherID int
	}

	Ignored struct{} `db:"-"`
}

func TestFieldsOnly(t *testing.T) {
	s := Fields(&fieldsModel{})

	want := map[string]bool{
		"ID":       false,
		"Username": false,
	}
	s.Only("ID", "Username")

	checkFieldSelection(t, s, want)
}

func TestFieldsExcept(t *testing.T) {
	s := Fields(&fieldsModel{})

	want := map[string]bool{
		"ID":        false,
		"Username":  false,
		"FirstName": false,
		"LastName":  false,
	}
	s.Except("Age", "OtherID")

	checkFieldSelection(t, s, want)
}

func checkFieldSelection(t *testing.T, s *FieldSelector, want map[string]bool) {
	for _, field := range s.Select() {
		alreadySelected, inSet := want[field.Name]
		if !inSet {
			t.Errorf("Unwanted field %q in selection", field.Name)
		} else if alreadySelected {
			t.Errorf("Field %q already selected", field.Name)
		}
		want[field.Name] = true
	}
	for field, selected := range want {
		if !selected {
			t.Errorf("Field %q must be selected but it was not", field)
		}
	}
}
