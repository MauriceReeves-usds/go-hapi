package hl7Utilities

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Segment interface {
	MSH | SFT
	New(rawSegment string)
}

type Terser interface {
	Get(specification string) (*string, error)
	MessageSegments() []string
	Preprocess() MSH
}

type FieldIndex struct {
	Index  int64
	Repeat int64
}

type TerserSpecification struct {
	Segment      string
	SetId        int64
	FieldIndices []FieldIndex
}

type MSH struct {
	EncodingCharacters string
	FieldSeparator     string
	Version            string
	MessageEvent       string
	MessageParts       []string
}

type SFT struct {
}

type Hl7Message struct {
	RawMessage         string
	version            string
	encodingCharacters string
	messageEvent       string
	fieldSeparator     string
	MSH
}

// Preprocess - reads a message and gets the important metadata from the MSH header
func (message Hl7Message) Preprocess() (MSH, error) {
	// split the message into its segments
	segments := message.MessageSegments()
	// we need to look at the MSH message because that tells us what the encoding characters are
	// and what the field separator are
	msh := segments[0]
	if msh[0:3] != "MSH" {
		return MSH{}, errors.New(fmt.Sprintf("HL7 message does not start with MSH"))
	}
	// remove the `MSH` value from the front
	msh = msh[3:]
	// remove the separator, which defaults to "|" but could be ANYTHING really
	separator := msh[0:1]
	// split the value by the separator now
	mshParts := strings.Split(msh, separator)
	// but add an empty string to the front to stand in for the separator. bad design choices by
	// the HL7 team, but so be it
	mshParts = append([]string{""}, mshParts...)
	// get the encoding characters, typically `^~\&`
	encodingCharacters := mshParts[2]
	// get the first subfield separator
	subfieldSeparator := encodingCharacters[0:1]
	messageEvent := mshParts[9]
	if strings.Contains(messageEvent, subfieldSeparator) {
		eventParts := strings.Split(messageEvent, subfieldSeparator)
		switch len(eventParts) {
		case 3:
			messageEvent = eventParts[2]
		case 2:
			messageEvent = fmt.Sprintf("%s_%s", eventParts[0], eventParts[1])
		default:
			return MSH{}, errors.New(fmt.Sprintf("unable to determine event type from %s", messageEvent))
		}
	}
	version := mshParts[12]
	// return our MSH object
	return MSH{encodingCharacters, separator, version, messageEvent, mshParts}, nil
}

// MessageSegments - return the raw message split by our delimiter
func (message Hl7Message) MessageSegments() []string {
	// trim off the spaces
	hl7Message := strings.TrimSpace(message.RawMessage)
	// replace any \r with \n because the HL7 spec requires \r as the delimiter
	hl7Message = strings.ReplaceAll(hl7Message, "\r", "\n")
	// split the string
	return strings.Split(hl7Message, "\n")
}

// Get - implement the interface
func (message Hl7Message) Get(specification string) (*string, error) {
	// do some sanity-checking on the specification
	if len(specification) == 0 {
		return nil, errors.New("invalid specification passed in")
	}
	if len(specification) == 0 && specification != "." {
		return nil, errors.New(fmt.Sprintf("invalid specification passed in: %s", specification))
	}
	// return the whole message
	if specification == "." {
		return &message.RawMessage, nil
	}
	// split the message into its segments
	segments := message.MessageSegments()
	// we need to look at the MSH message because that tells us what the encoding characters are
	// and what the field separator are
	msh, err := message.Preprocess()
	if err != nil {
		return nil, errors.Unwrap(err)
	}
	// set the encoding characters for the message
	message.encodingCharacters = msh.EncodingCharacters
	message.messageEvent = msh.MessageEvent
	message.version = msh.Version
	// split our encoding characters into a slice so we can use each one
	encodingChars := strings.Split(msh.EncodingCharacters, "")
	// parse our specification
	terserSpec, err := parseTerserSpecification(specification)
	if err != nil {
		return nil, errors.Unwrap(err)
	}
	// we need to find our segment. this is a hack to replace the segment with the MSH one we preprocessed
	// versus the one we match by segment name (OBR, ORC, etc)
	var targetSegment []string
	// we already have an MSH segment set up
	if terserSpec.Segment == "MSH" {
		targetSegment = msh.MessageParts
	} else {
		// find our matching segment
		matchingSegment, err := findSegment(segments, terserSpec.Segment, terserSpec.SetId, msh.FieldSeparator)
		if err != nil {
			panic(err)
		}
		targetSegment = strings.Split((matchingSegment)[3:], msh.FieldSeparator)
	}
	// load the first value
	fieldIndex := terserSpec.FieldIndices[0]
	value := targetSegment[fieldIndex.Index]
	// handle repetition
	if fieldIndex.Repeat != 0 {
		repeatedFields := strings.Split(value, "~")
		if int64(len(repeatedFields)) >= fieldIndex.Repeat {
			value = repeatedFields[fieldIndex.Repeat]
		}
	}
	// loop through the indices and split each time resetting the value of `value` based on the encoding char
	for i, fieldIndex := range terserSpec.FieldIndices[1:] {
		if fieldIndex.Repeat != 0 {
			repeatedFields := strings.Split(value, "~")
			if int64(len(repeatedFields)) >= fieldIndex.Repeat {
				value = repeatedFields[fieldIndex.Repeat]
			}
		}
		list := strings.Split(value, encodingChars[i])
		value = list[fieldIndex.Index-1]
	}
	return &value, nil
}

// findSegment - looks for the segment in a list
func findSegment(segments []string, segment string, repeat int64, fieldSeparator string) (string, error) {
	if repeat == 1 {
		for _, s := range segments {
			if s[0:3] == segment {
				return s, nil
			}
		}
	} else {
		for _, s := range segments {
			if s[0:3] == segment {
				segmentParts := strings.Split(s, fieldSeparator)
				segmentNumber, err := strconv.ParseInt(segmentParts[1], 10, 0)
				if err != nil {
					return "", errors.New(
						fmt.Sprintf(
							"requested %d repeat of %s but that segment does not support repeating",
							repeat,
							segment,
						),
					)
				}
				if segmentNumber == repeat {
					return s, nil
				}
			}
		}
	}

	return "", errors.New(fmt.Sprintf("segment matching %s not found", segment))
}

func parseTerserSpecification(specification string) (TerserSpecification, error) {
	var fieldIndices []FieldIndex
	// get the segment the specification is for
	segment := specification[0:3]
	// get the rest of the specification
	remainder := specification[3:]
	// split the remainder based on pipe
	specParts := strings.Split(remainder, "-")
	// create a default repeat value, which should be 1
	var repeat int64 = 1
	// check to see if we're looking for a repeatable field, like SFT(2)-1-1 vs MSH-10
	// if someone tried to request the second MSH segment, we'll just return nil
	if strings.Contains(specParts[0], "(") {
		// we are dealing with a repeat value here, for example NTE(3)-1
		if len(specParts) == 1 {
			// in this case, we're looking at an invalid spec like NTE(3), where spec parts would be ["(3)"]
			// this is a meaningless spec
			return TerserSpecification{}, errors.New(fmt.Sprintf("invalid specification passed in: %s", specification))
		}

		setId := specParts[0]
		setId = setId[1:]
		if strings.LastIndex(setId, ")") > -1 {
			setId = setId[0:strings.LastIndex(setId, ")")]
		}
		value, err := strconv.ParseInt(setId, 10, 0)
		if err != nil {
			newError := fmt.Errorf("error parsing desired repeat [%w]", err)
			return TerserSpecification{}, errors.Unwrap(newError)
		}
		// set our variable
		repeat = value
		// pop off the set id piece
		specParts = specParts[1:]
	}

	for _, s := range specParts {
		if s == "" {
			continue
		}
		fieldIndices = append(fieldIndices, parseFieldIndex(s))
	}

	return TerserSpecification{segment, repeat, fieldIndices}, nil
}

// parseFieldIndex takes a specification and breaks it apart into the field index AND the repeat
// value, which defaults to 0, which is the first or only item
func parseFieldIndex(specPart string) FieldIndex {
	if strings.Contains(specPart, "(") && strings.Contains(specPart, ")") {
		repeatStart := strings.Index(specPart, "(")
		repeatEnd := strings.Index(specPart, ")")
		indexStr := specPart[0:repeatStart]
		repeatStr := specPart[repeatStart+1 : repeatEnd]
		index, _ := strconv.ParseInt(indexStr, 10, 0)
		repeat, _ := strconv.ParseInt(repeatStr, 10, 0)
		return FieldIndex{index, repeat}
	} else {
		v, _ := strconv.ParseInt(specPart, 10, 0)
		return FieldIndex{v, 0}
	}
}
