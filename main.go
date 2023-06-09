package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Log struct {
	RemoteUser  *string `json:"remoteUser"`
	CountryCode string  `json:"country_code"`
}

func main() {
	// Directory and file mask
	directory := "channels"
	fileMask := "*.log*"

	// Create the output file
	outputFile, err := os.Create("output.txt")
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()

	// Data structure to store unique lines
	uniqueLines := make(map[string]bool)

	// Get the list of files matching the file mask
	files, err := filepath.Glob(filepath.Join(directory, fileMask))
	if err != nil {
		fmt.Println("Error getting file list:", err)
		return
	}

	// Process each file
	for _, file := range files {
		// Open the file for reading
		inputFile, err := os.Open(file)
		if err != nil {
			fmt.Println("Error opening file for reading:", err)
			continue
		}
		defer inputFile.Close()

		// Check the file extension
		ext := filepath.Ext(file)
		if ext == ".gz" {
			// Create a gzip reader for decompressing the file
			gzReader, err := gzip.NewReader(inputFile)
			if err != nil {
				fmt.Println("Error creating gzip reader:", err)
				continue
			}
			defer gzReader.Close()

			// Use the gzip reader as the input reader
			processLogFile(gzReader, uniqueLines)
		} else {
			// Use the regular file as the input reader
			processLogFile(inputFile, uniqueLines)
		}
	}

	// Write the unique lines to the output file
	for line := range uniqueLines {
		_, err := outputFile.WriteString(line)
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}
	}
}

// ProcessLogFile reads the log file line by line and extracts the desired fields
func processLogFile(inputReader io.Reader, uniqueLines map[string]bool) {
	scanner := bufio.NewScanner(inputReader)
	for scanner.Scan() {
		line := scanner.Text()

		// Ignore empty lines
		if line == "" {
			continue
		}

		// Decode the JSON object from the line
		var log Log
		err := json.Unmarshal([]byte(line), &log)
		if err != nil {
			fmt.Println("Error decoding JSON:", err)
			continue
		}

		// Check if remoteUser is not empty and write the fields to the output file if they are unique
		if log.RemoteUser != nil && *log.RemoteUser != "" {
			outputLine := fmt.Sprintf("remoteUser: %s\ncountry_code: %s\n\n", *log.RemoteUser, log.CountryCode)

			// Check uniqueness of the line before writing
			if !uniqueLines[outputLine] {
				uniqueLines[outputLine] = true
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}
}
