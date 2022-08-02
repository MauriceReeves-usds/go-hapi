package hl7Utilities

import (
	"reflect"
	"strings"
	"testing"
)

const mshMessage = "MSH|^~\\&|Mayo Clinic RD^2.16.840.1.113883.3.2.12.1^ISO|Mayo Clinic DLMP^2.16.840.1.113883.3.2.12.1.1^ISO|" +
	"251-CDC-PRIORITY|251-CDC-PRIORITY|20220802003337-0500||ORU^R01^ORU_R01|2022080205333719454131|P|2.5.1|||NE|NE|USA||||" +
	"PHLabReport-NoAck^HL7^2.16.840.1.113883.9.11^ISO"

const simpleHl7Message = `
MSH|^~\&|Mayo Clinic RD^2.16.840.1.113883.3.2.12.1^ISO|Mayo Clinic DLMP^2.16.840.1.113883.3.2.12.1.1^ISO|251-CDC-PRIORITY|251-CDC-PRIORITY|20220802003337-0500||ORU^R01^ORU_R01|2022080205333719454131|P|2.5.1|||NE|NE|USA||||PHLabReport-NoAck^HL7^2.16.840.1.113883.9.11^ISO
SFT|Lawson^L^^^^MAYO RD&2.16.840.1.113883.3.2.12.1&ISO^XX^^^99999|19.1|Cloverleaf IE|9999||20101113
PID|1||M177323145^^^Mayo Clinic DLMP&2.16.840.1.113883.3.2.12.1.1&ISO^PI^MCLab-RO Main Campus&2.16.840.1.113883.3.2.12.1.2.1&ISO||LASTNAME^FIRSTNAME^MIDDLE|MAIDEN|19000101|M|ALIAS|UNK^UNKNOWN^HL70005^U^UNKNOWN^L^2.5.1^4|STREET1^STREET2^CITY^LA^70461^COUNTRY^^^COUNTY|||||||||||U^UNKNOWN^HL70189^U^UNKNOWN^L^2.5.1^4|||||||||||||337915000^Homo sapiens (organism)^SCT^human^human^L^07/31/2012^4
ORC|RE|B523004918^Placer Order Number^2.16.840.1.113883.3.2.12.1.99^ISO|H823018568^Filler Order Number^2.16.840.1.113883.3.2.12.1.1^ISO|||||||||NPI^NEGROTTO GUNTHER^KATHERINE^^^^^^Ochsner Medical Cent&2.16.840.1.113883.3.2.12.1.99&ISO^L^^^PRN^Mayo Clinic DLMP&2.16.840.1.113883.3.2.12.1.1&ISO^^^^^^^MD|7018377|^^^^^^|||||||Ochsner Medical Center North Shore|100 Medical Center Dr^^Slidell^LA^704615520|^WPN^PH^^1^985^6465060
OBR|1|B523004918^Placer Order Number^2.16.840.1.113883.3.2.12.1.99^ISO|H823018568^Filler Order Number^2.16.840.1.113883.3.2.12.1.1^ISO|^^^MPXDX^Orthopoxvirus DNA, PCR, Swab^L^^U|||202207231050|||7018377^Ochsner Medical Center North Shore^9856465060|||||^^groin|NPI^NEGROTTO GUNTHER^KATHERINE^^^^^^Ochsner Medical Cent&2.16.840.1.113883.3.2.12.1.99&ISO^L^^^PRN^Mayo Clinic DLMP&2.16.840.1.113883.3.2.12.1.1&ISO^^^^^^^MD||||||20220801145700-0500|||F
OBX|1|CE|100434-0^Orthopoxvirus.non-variola DNA XXX Ql NAA+non-probe^LN^618596^Orthopoxvirus DNA, PCR^L^2.40^U||260415000^Undetected^SCT||Undetected||||F|||202207231050|24D0404292^Mayo Clinic Labs-Roch Main Campus^L||||20220801145700-0500||||MCLab-RO Main Campus^A^^^^CLIA&2.16.840.1.113883.4.7&ISO^XX^^^24D0404292|530 Hilton^Level 1^Rochester^MN^55905^USA^L
NTE|1|L|Non-variola Orthopoxvirus DNA is not detected by real-time 
NTE|2|L|PCR primer and probe set.
SPM|1|B523004918&Placer_LIS&2.16.840.1.113883.3.2.12.1.99&ISO^H823018568&Mayo_LIS&2.16.840.113883.1.3.2.11.1&ISO||^^^groin^groin^L^^v1|||||||P^Patient^HL70369^P^Patient^L^2.5.1^V1|1^{#}&Number&UCUM&unit&unit&L&1.1&V1|||||20220723105000-0500|20220726152000-0500
`

func TestHl7Message_Get(t *testing.T) {
	// test some MSH only values
	cases := []struct{ spec, expectedValue string }{
		{"MSH-3", "Mayo Clinic RD^2.16.840.1.113883.3.2.12.1^ISO"},
		{"MSH-3-1", "Mayo Clinic RD"},
		{"MSH-3-2", "2.16.840.1.113883.3.2.12.1"},
		{"MSH-3-3", "ISO"},
		{"MSH-9", "ORU^R01^ORU_R01"},
		{"MSH-9-1", "ORU"},
		{"MSH-9-2", "R01"},
		{"MSH-9-3", "ORU_R01"},
	}
	hl7Message := Hl7Message{RawMessage: mshMessage}
	for _, c := range cases {
		value, err := hl7Message.Get(c.spec)
		if err != nil {
			t.Log("error should be nil", err)
			t.Fail()
		}
		if *value != c.expectedValue {
			t.Logf("Value should be '%s' but got '%s'", c.expectedValue, *value)
			t.Fail()
		}
	}
	// now check a longer message
	hl7Message = Hl7Message{RawMessage: simpleHl7Message}
	value, err := hl7Message.Get("SFT-3")
	if err != nil {
		t.Log("error should be nil", err)
		t.Fail()
	}
	if *value != "Cloverleaf IE" {
		t.Logf("Value should be '%s' but got '%s'", "Cloverleaf IE", *value)
		t.Fail()
	}
}

// tests the findSegment method and lets us verify its functionality
func Test_findSegment(t *testing.T) {
	// mock the available segments, representing a standard ORU_R01 message
	segments := []string{"MSH||", "SFT||", "PID|1|", "ORC|RE|", "OBR|1|", "OBR|2|", "OBX|1|", "NTE|1|", "NTE|2|", "SPM|1|"}
	// our args that will get passed into the func
	type args struct {
		segments       []string
		segment        string
		repeat         int64
		fieldSeparator string
	}
	// our test cases
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"MSH test pass", args{segments, "MSH", 1, "|"}, "MSH||", false},
		{"SFT test pass", args{segments, "SFT", 1, "|"}, "SFT||", false},
		{"PID test pass", args{segments, "PID", 1, "|"}, "PID|1|", false},
		{"PID test fail", args{segments, "PID", 2, "|"}, "", true},
		{"ORC test pass", args{segments, "ORC", 1, "|"}, "ORC|RE|", false},
		{"ACK test fail", args{segments, "ACK", 1, "|"}, "", true},
	}
	// run all the tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := findSegment(tt.args.segments, tt.args.segment, tt.args.repeat, tt.args.fieldSeparator)
			if (err != nil) != tt.wantErr {
				t.Errorf("findSegment() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("findSegment() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHl7Message_Preprocess(t *testing.T) {
	// arrange
	const encodingCharacters = "^~\\&"
	const version = "2.5.1"
	const separator = "|"
	const messageEvent = "ORU_R01"
	var mshParts = strings.Split(mshMessage[3:], "|")
	mshParts = append([]string{""}, mshParts...)
	type fields struct {
		RawMessage         string
		version            string
		encodingCharacters string
		messageEvent       string
		fieldSeparator     string
		MSH                MSH
	}
	tests := []struct {
		name    string
		fields  fields
		want    MSH
		wantErr bool
	}{
		{"test base MSH case", fields{simpleHl7Message, version, encodingCharacters, messageEvent, separator, MSH{}}, MSH{encodingCharacters, separator, version, messageEvent, mshParts}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := Hl7Message{
				RawMessage:         tt.fields.RawMessage,
				version:            tt.fields.version,
				encodingCharacters: tt.fields.encodingCharacters,
				messageEvent:       tt.fields.messageEvent,
				fieldSeparator:     tt.fields.fieldSeparator,
				MSH:                tt.fields.MSH,
			}
			got, err := message.Preprocess()
			if (err != nil) != tt.wantErr {
				t.Errorf("Preprocess() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.MessageEvent != tt.want.MessageEvent ||
				got.FieldSeparator != tt.want.FieldSeparator ||
				got.EncodingCharacters != tt.want.EncodingCharacters ||
				got.Version != tt.want.Version {
				t.Errorf("Preprocess() got = %v, want %v", got, tt.want)
			}
			if len(got.MessageParts) != len(tt.want.MessageParts) {
				t.Errorf("Preprocess() fail on length - got = %v, want %v", got, tt.want)
			}
			for i, s := range got.MessageParts {
				if tt.want.MessageParts[i] != s {
					t.Errorf("Preprocess() fail on equality - %s != %s got = %v, want %v", s, tt.want.MessageParts[i], got, tt.want)
				}
			}
		})
	}
}

func Test_parseTerserSpecification(t *testing.T) {
	type args struct {
		specification string
	}
	tests := []struct {
		name    string
		args    args
		want    TerserSpecification
		wantErr bool
	}{
		{"test simple terser spec", args{"MSH-3-1"}, TerserSpecification{"MSH", 1, []FieldIndex{{3, 0}, {1, 0}}}, false},
		{"test terser spec with set", args{"PID(2)-13-1"}, TerserSpecification{"PID", 2, []FieldIndex{{13, 0}, {1, 0}}}, false},
		{"test terser spec with long repeat", args{"OBR(99)-1"}, TerserSpecification{"OBR", 99, []FieldIndex{{1, 0}}}, false},
		{"test terser spec with fail", args{"ZZZ(9)"}, TerserSpecification{}, true},
		{"test terser spec with repeated field", args{"PID-11(0)-5"}, TerserSpecification{"PID", 1, []FieldIndex{{11, 0}, {5, 0}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTerserSpecification(tt.args.specification)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTerserSpecification() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseTerserSpecification() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseFieldIndex(t *testing.T) {
	type args struct {
		specPart string
	}
	tests := []struct {
		name string
		args args
		want FieldIndex
	}{
		{"test basic field index", args{"11"}, FieldIndex{11, 0}},
		{"test basic field index with explicit zero", args{"11(0)"}, FieldIndex{11, 0}},
		{"test basic field index with repeat", args{"1(1)"}, FieldIndex{1, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseFieldIndex(tt.args.specPart); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseFieldIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}
