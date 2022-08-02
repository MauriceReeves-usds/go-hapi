package main

import (
	"fmt"
	"hl7Decomposer/hl7Utilities"
	"os"
	"path/filepath"
	"strings"
)

// main separator for HL7 fields
const fieldSeparator = "|"

// subfield separator for HL7
const subfieldSeparator = "^"

var results []map[string]string

// given an HL7 message from a file, clean it up, and convert it
// into an array of string
func getHl7MessageAsList(message string) []string {
	hl7Message := strings.TrimSpace(message)
	hl7Message = strings.ReplaceAll(hl7Message, "\r", "\n")
	// split the string
	return strings.Split(hl7Message, "\n")
}

// take the cleaned up message, split it, and then start processing it
func processHl7Message(hl7Message string) {
	var values map[string]string
	messageParts := getHl7MessageAsList(hl7Message)
	for _, s := range messageParts {
		cleaned := strings.TrimSpace(s)
		// skip empty lines
		if len(cleaned) == 0 {
			continue
		}
		// get the segment
		segment := cleaned[0:3]
		switch segment {
		case "MSH":
			// create our map
			values = make(map[string]string)
			msh := strings.Split(cleaned, fieldSeparator)
			values["message_date"] = msh[6]
			values["message_id"] = msh[9]
		case "OBX":
			obx := strings.Split(cleaned, fieldSeparator)
			testResult := strings.Split(obx[5], subfieldSeparator)
			values["test_result"] = testResult[1]
		case "OBR":
		case "PID":
			pid := strings.Split(cleaned, fieldSeparator)
			patientId := strings.Split(pid[3], subfieldSeparator)
			patientAddress := strings.Split(pid[11], subfieldSeparator)
			patientRace := strings.Split(pid[10], subfieldSeparator)
			values["patient_id"] = patientId[0]
			values["patient_dob"] = pid[7]
			values["patient_sex"] = pid[8]
			values["patient_state"] = patientAddress[3]
			values["patient_zip"] = patientAddress[4]
			values["patient_race"] = patientRace[1]
		case "SPM":
			// get spm values
			spm := strings.Split(cleaned, fieldSeparator)
			values["specimen_collection_date"] = spm[17]
			values["specimen_received_date"] = spm[18]
			// append to the array
			results = append(results, values)
		case "ORC":
		default:
			// skip NTE and SFT and headers and footers oh my
			continue
		}
	}
}

// checks for an error on a result, like reading a file, etc
func check(e error) {
	if e != nil {
		panic(e)
	}
}

// reads the file and returns a string of the contents
// this is very naive because if the file was very big it could
// take a long time
func readFile(filePath string) string {
	data, err := os.ReadFile(filePath)
	check(err)
	return string(data)
}

// recurse some directories
func walkResultsDirs(path string) {
	var hl7Message string
	dir, err := os.ReadDir(path)
	check(err)
	for _, entry := range dir {
		if entry.IsDir() {
			// recurse in
			walkResultsDirs(filepath.Join(path, entry.Name()))
		} else {
			fileName := entry.Name()
			ext := strings.ToLower(filepath.Ext(fileName))
			if ext == ".dat" {
				fmt.Println(fileName)
				fullPath := filepath.Join(path, fileName)
				hl7Message = readFile(fullPath)
				processHl7Message(hl7Message)
			}
		}
	}
}

// our main method
func main() {
	// open our folder
	dirPath := "/Users/maurice/Downloads/hl7/mayo-clinic/"
	// walk the directory
	walkResultsDirs(dirPath)
	// output our decomposed list of values
	for _, entry := range results {
		fmt.Println(entry)
	}
	hl7Message := hl7Utilities.Hl7Message{RawMessage: "MSH|^~\\&|Mayo Clinic RD^2.16.840.1.113883.3.2.12.1^ISO|Mayo Clinic DLMP^2.16.840.1.113883.3.2.12.1.1^ISO|251-CDC-PRIORITY|251-CDC-PRIORITY|20220728002328-0500||ORU^R01^ORU_R01|2022072805232819443001|T|2.5.1|||NE|NE|USA||||PHLabReport-NoAck^HL7^2.16.840.1.113883.9.11^ISO"}
	fmt.Println(hl7Message)
	v, err := hl7Message.Get("MSH-9-3")
	check(err)
	fmt.Println(*v)
}
