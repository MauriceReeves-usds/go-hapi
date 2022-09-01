package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// main separator for HL7 fields
const fieldSeparator = "|"

// subfield separator for HL7
const subfieldSeparator = "^"

// date formats in Go are kind of...dumb. you specify the format for your date
// by telling Go what the data 02-Jan 3:04:05 PM 2006 -0700 looks like when parsing dates
// yes really the date is essentially 1 2 3 4 5 6 7 meaning
// 1		2		3		4			5			6					7
// month	day		hour	minute		second		year (as 2006)		offset (in negative)
const longDateFormat = "20060102150405-0700"

const shortDateFormat = "20060102"

var results []map[string]string

// keys - returns the keys for a map
func keys[K string, V string](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func parseDate(date string) (time.Time, error) {
	var parsedDate time.Time
	var err error
	if len(date) == 8 {
		parsedDate, err = time.Parse(shortDateFormat, date)
	} else {
		parsedDate, err = time.Parse(longDateFormat, date)
	}
	return parsedDate, err
}

func getPatientAge(patientDob, messageDate string) float64 {
	reportingDate, err := parseDate(messageDate)
	check(err)
	var dob time.Time
	dob, err = parseDate(patientDob)
	check(err)
	timeBetween := reportingDate.Sub(dob)
	return timeBetween.Minutes() / (60 * 24 * 365)
}

// given an HL7 message from a file, clean it up, and convert it
// into an array of string
func getHl7MessageAsList(message string) []string {
	hl7Message := strings.TrimSpace(message)
	hl7Message = strings.ReplaceAll(hl7Message, "\r", "\n")
	// split the string
	return strings.Split(hl7Message, "\n")
}

// take the cleaned up message, split it, and then start processing it
func processHl7Message(hl7Message, fileName string) {
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
			// add the file name to the CSV
			values["file_name"] = fileName
			msh := strings.Split(cleaned, fieldSeparator)
			// get the sender ID from MSH-3
			labName := strings.Split(msh[2], subfieldSeparator)[0]
			// normalize the lab names since the MSH fields can have different values
			switch strings.ToLower(labName) {
			case "mayo clinic rd":
				values["sender_id"] = "mayo"
			case "corep.sonichealth.pr", "corep.sonichealth.st":
				values["sender_id"] = "sonic"
			case "aegis":
				values["sender_id"] = "aegis"
			default:
				values["sender_id"] = labName
			}
			values["lab_name"] = values["sender_id"]
			values["message_date"] = msh[6]
			values["reporting_date"] = msh[6]
			values["message_id"] = msh[9]
		case "OBX":
			obx := strings.Split(cleaned, fieldSeparator)
			// skip any AOEs
			if obx[1] != "1" {
				continue
			}
			testResult := strings.Split(obx[5], subfieldSeparator)
			values["test_result"] = testResult[1]
		case "OBR":
		case "PID":
			pid := strings.Split(cleaned, fieldSeparator)
			patientId := strings.Split(pid[3], subfieldSeparator)
			patientAddress := strings.Split(pid[11], subfieldSeparator)
			patientRace := strings.Split(pid[10], subfieldSeparator)
			patientEthnicity := strings.Split(pid[22], subfieldSeparator)
			// get the patient age
			age := getPatientAge(pid[7], values["message_date"])
			values["pt_id"] = patientId[0]
			values["pt_dob"] = pid[7]
			values["pt_age"] = fmt.Sprintf("%.0f", math.Abs(math.Floor(age)))
			values["pt_sex"] = pid[8]
			values["pt_state"] = strings.TrimSpace(patientAddress[3])
			if len(patientRace) > 1 {
				values["pt_race"] = strings.TrimSpace(patientRace[1])
			} else {
				values["pt_race"] = strings.TrimSpace(patientRace[0])
			}
			if len(patientEthnicity) > 1 {
				values["pt_ethnicity"] = strings.TrimSpace(patientEthnicity[1])
			} else {
				values["pt_ethnicity"] = strings.TrimSpace(patientEthnicity[0])
			}

		case "SPM":
			// get spm values
			spm := strings.Split(cleaned, fieldSeparator)
			//values["specimen_collection_date"] = spm[17]
			//values["specimen_received_date"] = spm[18]
			specimenType := strings.Split(spm[4], subfieldSeparator)
			if len(specimenType) > 0 {
				if values["lab_name"] == "mayo" && len(specimenType) == 8 {
					values["specimen_type"] = strings.ToUpper(strings.TrimSpace(specimenType[3]))
				} else {
					values["specimen_type"] = strings.ToUpper(strings.TrimSpace(specimenType[1]))
				}
			} else {
				values["specimen_type"] = ""
			}
			values["specimen_collection_date"] = spm[17]
			values["specimen_received_date"] = spm[18]
			// append to the array
			results = append(results, values)
		case "ORC":
			orc := strings.Split(cleaned, fieldSeparator)
			orderingProviderAddress := strings.Split(orc[22], subfieldSeparator)
			values["ordering_provider_state"] = orderingProviderAddress[3]
			orderingFacilityName := orc[21]
			// these apparently contain the caret sometimes, which is silly. strip it rabbit
			if strings.Index(orderingFacilityName, subfieldSeparator) > 0 {
				nameParts := strings.Split(orderingFacilityName, subfieldSeparator)
				orderingFacilityName = nameParts[0]
			}
			values["ordering_facility_name"] = strings.ToUpper(strings.TrimSpace(orderingFacilityName))
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
func walkResultsDirs(path string, extension string) {
	var hl7Message string
	dir, err := os.ReadDir(path)
	check(err)
	for _, entry := range dir {
		if entry.IsDir() {
			// recurse in
			walkResultsDirs(filepath.Join(path, entry.Name()), extension)
		} else {
			fileName := entry.Name()
			ext := strings.ToLower(filepath.Ext(fileName))
			if ext == extension {
				fmt.Println(fileName)
				fullPath := filepath.Join(path, fileName)
				hl7Message = readFile(fullPath)
				processHl7Message(hl7Message, fileName)
			}
		}
	}
}

// our main method
func main() {
	// this is our collection of paths to check
	var paths = map[string]string{
		"/Users/maurice/Downloads/hl7/":              ".dat",
		"/Users/maurice/Downloads/hl7/raw-hl7":       ".hl7",
		"/Users/maurice/Downloads/hl7/aegis/raw-hl7": ".hl7",
	}
	// loop the map and process
	for dirPath, extension := range paths {
		// walk the directory
		walkResultsDirs(dirPath, extension)
	}
	// open our output folder
	dirPath := "/Users/maurice/Downloads/hl7/"
	// output our decomposed list of values
	for _, entry := range results {
		fmt.Println(entry)
	}
	file, err := os.Create(dirPath + "results.csv")
	check(err)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	fmt.Printf("keys in our results: %v\n", keys(results[0]))
	keys := keys(results[0])
	// write out our headers
	writer.Write(keys)
	// decompose
	for _, entry := range results {
		l := make([]string, 0, len(keys))
		for _, key := range keys {
			l = append(l, entry[key])
		}
		writer.Write(l)
	}
}
