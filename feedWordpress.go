package main

import (
	"io/ioutil"
	"fmt"
	"os"
	"path/filepath"
	"cartemaritime/common"
	"path"
	"sort"
	"strings"
	"encoding/xml"
)

func main() {
	rawDataChan := readRawData(common.DATADIR)


	for data := range rawDataChan {
		fmt.Println(data)

		metadata, err := parseXML(data.xmlName)

		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}

		fmt.Println(metadata)
	}
}

type rawData struct {
	name string
	hdImageName string
	xmlName string
}

// Read the given directory and return a channel of rawData for each subdir
func readRawData(dirname string) <-chan rawData {

	dirEntries, err := ioutil.ReadDir(dirname)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// reformat entry name (12_345 --> 000012_345)
	formatter := func(name string) string {
		splitted := strings.Split(name, "_")
		if len(splitted) != 2 {
			return name
		}
		return fmt.Sprintf("%06s_%s", splitted[0], splitted[1])
	}

	// Custom sort of entries
	sort.Slice(dirEntries, func(i, j int) bool {
		return formatter(dirEntries[i].Name()) < formatter(dirEntries[j].Name())
	})

	out := make(chan rawData)

	go func() {
		defer close(out)

		for _, entry := range dirEntries {

			if ! entry.IsDir() {
				continue
			}
			
			dirPath := path.Join(dirname, entry.Name())

			hdImageFile, err := findFile(dirPath, ".jp2")
			if err != nil {
				fmt.Println(err)
				continue
			}

			xmlFile, err := findFile(dirPath, ".xml")
			if err != nil {
				fmt.Println(err)
				continue
			}

			data := rawData{
				name: entry.Name(),
				hdImageName: path.Join(dirPath, hdImageFile.Name()),
				xmlName: path.Join(dirPath, xmlFile.Name()),
			}

			out <- data
		}

	}()

	return out
}

// Find the first file with the given extension
func findFile(dirname string, ext string) (os.FileInfo, error) {
	dirEntries, err := ioutil.ReadDir(dirname)

	if err != nil {
		return nil, err
	}

	for _, entry := range dirEntries {
		if filepath.Ext(entry.Name()) == ext {
			return entry, nil
		}

		entry.Name()
	}

	return nil, fmt.Errorf("file not found")
}


type Metadata struct {
	DateStamp string `xml:"dateStamp>DateTime"`
	ReferenceSystem string `xml:"referenceSystemInfo>MD_ReferenceSystem>referenceSystemIdentifier>RS_Identifier>code>CharacterString"`
	Title string `xml:"identificationInfo>MD_DataIdentification>citation>CI_Citation>title>CharacterString"`
	Date string `xml:"identificationInfo>MD_DataIdentification>citation>CI_Citation>date>CI_Date>date>Date"`

	CitedParties []CitedParty `xml:"identificationInfo>MD_DataIdentification>citation>CI_Citation>citedResponsibleParty"`

	Abstract string `xml:"identificationInfo>MD_DataIdentification>abstract>CharacterString"`

	Keywords []string `xml:"identificationInfo>MD_DataIdentification>descriptiveKeywords>MD_Keywords>keyword>CharacterString"`

	Extent Extent `xml:"identificationInfo>MD_DataIdentification>extent>EX_Extent>geographicElement>EX_GeographicBoundingBox"`
}

type CitedParty struct {

	// One of the two next
	Person string `xml:"CI_ResponsibleParty>individualName>CharacterString"`
	Org string `xml:"CI_ResponsibleParty>organisationName>CharacterString"`

	Role Role `xml:"CI_ResponsibleParty>role>CI_RoleCode"`
}

type Role struct {
	Role string `xml:"codeListValue,attr"`
}

type Extent struct {
	WestBound string `xml:"westBoundLongitude>Decimal"`
	EastBound string `xml:"eastBoundLongitude>Decimal"`
	SouthBound string `xml:"southBoundLatitude>Decimal"`
	NorthBound string `xml:"northBoundLatitude>Decimal"`
}

func parseXML(filename string) (Metadata, error) {
	xmlFile, err := os.Open(filename)
	if err != nil {
		return Metadata{}, err
	}

	defer xmlFile.Close()

	byteValue, _ := ioutil.ReadAll(xmlFile)

	var metadata Metadata
	xml.Unmarshal(byteValue, &metadata)

	metadata = cleanCitedParty(metadata)

	return metadata, nil
}

func cleanCitedParty(metadata Metadata) Metadata {
	cleaned := make([]CitedParty, 0, len(metadata.CitedParties))

	for _, party := range metadata.CitedParties {
		if len(party.Person) > 0 {
			cleaned = append(cleaned, CitedParty{
				Person: party.Person,
				Role: party.Role,
			})
		}

		if len(party.Org) > 0 {
			cleaned = append(cleaned, CitedParty{
				Org: party.Org,
				Role: party.Role,
			})
		}

		// Drop malformed cited party from the XML (no name at all)
	}

	metadata.CitedParties = cleaned

	return metadata
}