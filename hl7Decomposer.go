package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
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

const mediumDateFormat = "200601021504"

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

// parseDate - Given a string, try to parse the dates per the formats we know
// in these files and return a [time.Time] object
func parseDate(date string) (time.Time, error) {
	var parsedDate time.Time
	var err error
	// put some bumpers around the date value
	if strings.Contains(date, subfieldSeparator) {
		// mayo is now sending the age! but it blows up this logic
		dateParts := strings.Split(date, subfieldSeparator)
		// strip off the date portion
		date = dateParts[0]
	}
	if len(date) == 8 {
		parsedDate, err = time.Parse(shortDateFormat, date)
	} else if len(date) == 12 {
		parsedDate, err = time.Parse(mediumDateFormat, date)
	} else {
		parsedDate, err = time.Parse(longDateFormat, date)
	}
	return parsedDate, err
}

// getPatientAge - given two string representations of dates, parse them
// and then get the distance between as a float64 value representing years
// however, Mayo now sends us the age as part of the DOB, so I have to split
// that off and if we have it, and it's a real numeric value, return that instead
func getPatientAge(patientDob, messageDate string) float64 {
	if strings.Contains(patientDob, subfieldSeparator) {
		// this should be the date portion AND the age portion. it will look like 19000101^30Y
		// this is non-standard HL7, so we need to handle it manually here
		patientDobParts := strings.Split(patientDob, subfieldSeparator)
		rawPatientAge := patientDobParts[1]
		ageRegex := regexp.MustCompile(`^\d+`)
		matches := ageRegex.FindAllString(rawPatientAge, -1)
		if len(matches) > 0 {
			// ignore the error
			age, _ := strconv.ParseFloat(matches[0], 64)
			return age
		} else {
			return 122
		}
	} else {
		reportingDate, err := parseDate(messageDate)
		check(err)
		var dob time.Time
		dob, err = parseDate(patientDob)
		check(err)
		timeBetween := reportingDate.Sub(dob)
		return timeBetween.Minutes() / (60 * 24 * 365)
	}
}

func parseAndFormatDate(rawDate string) string {
	date, err := parseDate(rawDate)
	if err != nil {
		fmt.Printf("Error parsing date: %v\n", err)
		return rawDate
	}
	return date.Format(longDateFormat)
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
			case "aegis", "horizon":
				values["sender_id"] = "aegis"
			default:
				values["sender_id"] = labName
			}
			values["lab_name"] = values["sender_id"]
			values["message_date"] = parseAndFormatDate(msh[6])
			values["reporting_date"] = msh[6]
			values["message_id"] = msh[9]
		case "OBX":
			obx := strings.Split(cleaned, fieldSeparator)
			// skip any AOEs
			if obx[1] == "1" && (obx[2] == "CE" || obx[2] == "CWE") {
				testResult := strings.Split(obx[5], subfieldSeparator)
				values["test_result"] = testResult[1]
			} else {
				// parse out the patient age
				if obx[2] == "NM" && obx[3] == "30525-0" {
					patientAge, err := strconv.ParseInt(obx[5], 0, 64)
					if err == nil {
						values["patient_age"] = fmt.Sprintf("%d", patientAge)
					} else {
						values["patient_age"] = "122"
					}
				}
			}
		case "OBR":
		case "PID":
			pid := strings.Split(cleaned, fieldSeparator)
			patientId := strings.Split(pid[3], subfieldSeparator)
			patientAddress := strings.Split(pid[11], subfieldSeparator)
			patientRace := strings.Split(pid[10], subfieldSeparator)
			patientEthnicity := strings.Split(pid[22], subfieldSeparator)
			// get the patient age
			age := getPatientAge(pid[7], values["message_date"])
			dob, _ := parseDate(pid[7])
			values["pt_id"] = patientId[0]
			values["pt_dob"] = dob.Format("20060102") // see other notes about Go date formatting
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
			if len(spm) >= 18 {
				values["specimen_collection_date"] = parseAndFormatDate(spm[17])
				values["specimen_received_date"] = parseAndFormatDate(spm[18])
			} else {
				values["specimen_collection_date"] = values["message_date"]
				values["specimen_received_date"] = values["message_date"]
			}
			// append to the array
			results = append(results, values)
		case "ORC":
			orc := strings.Split(cleaned, fieldSeparator)
			// get the accession number
			fillerOrderNumber := strings.Split(orc[3], subfieldSeparator)
			values["filler_order_number"] = strings.TrimSpace(strings.ToUpper(fillerOrderNumber[0]))
			orderingFacilityName := orc[21]
			// these apparently contain the caret sometimes, which is silly. strip it rabbit
			if strings.Index(orderingFacilityName, subfieldSeparator) > 0 {
				nameParts := strings.Split(orderingFacilityName, subfieldSeparator)
				orderingFacilityName = nameParts[0]
			}
			// get the ordering facility information
			orderingFacilityAddress := strings.Split(orc[22], subfieldSeparator)
			values["ordering_facility_state"] = strings.TrimSpace(orderingFacilityAddress[3])
			values["ordering_facility_zip"] = strings.TrimSpace(orderingFacilityAddress[4])
			if len(orderingFacilityAddress) >= 9 {
				values["ordering_facility_county"] = strings.TrimSpace(orderingFacilityAddress[8])
			} else {
				values["ordering_facility_county"] = ""
			}
			values["ordering_facility_name"] = strings.ToUpper(strings.TrimSpace(orderingFacilityName))
			// now the ordering provider information. not every lab sends us this, so we can default this
			// to be the ordering facility information if it doesn't exist
			orderingProviderName := strings.Split(orc[12], subfieldSeparator)
			providerName := fmt.Sprintf(
				"%s %s",
				strings.TrimSpace(orderingProviderName[2]),
				strings.TrimSpace(orderingProviderName[1]),
			)
			values["ordering_provider_name"] = strings.ToUpper(providerName)
			if len(orc) >= 25 {
				orderingProviderAddress := strings.Split(orc[24], subfieldSeparator)
				values["ordering_provider_state"] = strings.TrimSpace(orderingProviderAddress[3])
				values["ordering_provider_zip"] = strings.TrimSpace(orderingProviderAddress[4])
				if len(orderingProviderAddress) >= 9 {
					values["ordering_provider_county"] = strings.TrimSpace(orderingProviderAddress[8])
				} else {
					values["ordering_provider_county"] = ""
				}
			} else {
				values["ordering_provider_state"] = values["ordering_facility_state"]
				values["ordering_provider_zip"] = values["ordering_facility_zip"]
				values["ordering_provider_county"] = values["ordering_facility_county"]
			}
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
	// get a count of each lab's results
	countAegis, countSonic, countMayo, countOther := 0, 0, 0, 0
	otherLabNames := make([]string, 0, 10)
	// output our decomposed list of values
	for _, entry := range results {
		// output the entry
		fmt.Println(entry)
		// get the count by lab name
		switch entry["lab_name"] {
		case "aegis":
			countAegis += 1
		case "sonic":
			countSonic += 1
		case "mayo":
			countMayo += 1
		default:
			countOther += 1
			otherLabNames = append(otherLabNames, entry["lab_name"])
		}
	}
	fmt.Printf("Aegis: %d, Sonic: %d, Mayo: %d, Other: %d\n", countAegis, countSonic, countMayo, countOther)
	fmt.Println(otherLabNames)
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
