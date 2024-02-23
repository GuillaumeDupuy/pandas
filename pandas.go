package pandas

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
)

// Pandas is a struct that holds the data of a pandas DataFrame
type Pandas struct {
	Columns []string
	Data    map[string][]interface{}
}

// NewDataFrame creates a new DataFrame with the given columns and data.
func NewDataFrame(columns []string, data map[string][]interface{}) *Pandas {
	return &Pandas{
		Columns: columns,
		Data:    data,
	}
}

// ReadCSV reads a CSV file and returns a Pandas struct representing the DataFrame.
func ReadCSV(filePath string) (*Pandas, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading headers: %v", err)
	}

	df := &Pandas{
		Columns: headers,
		Data:    make(map[string][]interface{}),
	}

	for _, header := range headers {
		df.Data[header] = make([]interface{}, 0)
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading record: %v", err)
		}

		for i, value := range record {
			// Attempt to parse the value into an int, then a float, and fall back to string if necessary
			if intValue, err := strconv.Atoi(value); err == nil {
				df.Data[headers[i]] = append(df.Data[headers[i]], intValue)
			} else if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
				df.Data[headers[i]] = append(df.Data[headers[i]], floatValue)
			} else {
				df.Data[headers[i]] = append(df.Data[headers[i]], value)
			}
		}
	}

	return df, nil
}

// ReadTXT reads a TXT file and returns a Pandas struct representing the DataFrame.
func ReadTXT(filePath string) (*Pandas, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading headers: %v", err)
	}

	df := &Pandas{
		Columns: headers,
		Data:    make(map[string][]interface{}),
	}

	for _, header := range headers {
		df.Data[header] = make([]interface{}, 0)
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading record: %v", err)
		}

		for i, value := range record {
			// Attempt to parse the value into an int, then a float, and fall back to string if necessary
			if intValue, err := strconv.Atoi(value); err == nil {
				df.Data[headers[i]] = append(df.Data[headers[i]], intValue)
			} else if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
				df.Data[headers[i]] = append(df.Data[headers[i]], floatValue)
			} else {
				df.Data[headers[i]] = append(df.Data[headers[i]], value)
			}
		}
	}

	return df, nil
}

// Head prints the first n rows of the DataFrame.
func (df *Pandas) Head(n int) {
	fmt.Println(df.Columns)
	for _, col := range df.Columns {
		if len(df.Data[col]) < n {
			fmt.Println(df.Data[col])
		} else {
			fmt.Println(df.Data[col][:n])
		}
	}
}

// Tail prints the last n rows of the DataFrame.
func (df *Pandas) Tail(n int) {
	fmt.Println(df.Columns)
	for _, col := range df.Columns {
		if len(df.Data[col]) < n {
			fmt.Println(df.Data[col])
		} else {
			fmt.Println(df.Data[col][len(df.Data[col])-n:])
		}
	}
}

// Display the index labels of the DataFrame.
func (df *Pandas) Index() {
	for i := 0; i < len(df.Data[df.Columns[0]]); i++ {
		fmt.Println(i)
	}
}

// Display the columns of the DataFrame
func (df *Pandas) Column() {
	fmt.Println(df.Columns)
}

// Print the shape of the DataFrame
func (df *Pandas) Shape() {
	fmt.Println("(", len(df.Columns), ",", len(df.Data[df.Columns[0]]), ")")
}

// isNumeric checks if the interface value is an int or float64 (common numeric types in Go).
func isNumeric(val interface{}) bool {
	switch val.(type) {
	case int, float64:
		return true
	default:
		return false
	}
}

// toFloat64 attempts to convert an interface value to float64 for calculation purposes.
func toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

// Describe provides a statistical summary of the DataFrame for numerical columns.
func (df *Pandas) Describe() {
	for _, col := range df.Columns {
		var sum, min, max float64
		var count int
		min = math.Inf(1)  // Initialize min to positive infinity
		max = math.Inf(-1) // Initialize max to negative infinity

		for _, val := range df.Data[col] {
			if isNumeric(val) {
				num, _ := toFloat64(val)
				sum += num
				count++
				if num < min {
					min = num
				}
				if num > max {
					max = num
				}
			}
		}

		if count > 0 { // Ensure column has numeric data
			mean := sum / float64(count)
			fmt.Printf("%s: count = %d, mean = %f, min = %f, max = %f\n", col, count, mean, min, max)
		} else {
			fmt.Printf("%s: No numeric data\n", col)
		}
	}
}

// Return the types of the columns
func (df *Pandas) Dtypes() {
	for _, col := range df.Columns {
		fmt.Printf("%s: %T\n", col, df.Data[col][0])
	}
}

// Sort object by labels (along an axis).
func (df *Pandas) Sort_index() {
	sortedColumns := make([]string, len(df.Columns))
	copy(sortedColumns, df.Columns)
	sort.Strings(sortedColumns)

	newData := make(map[string][]interface{})
	for _, col := range sortedColumns {
		newData[col] = df.Data[col]
	}

	df.Columns = sortedColumns
	df.Data = newData
}

func less(i, j interface{}) bool {
	switch i := i.(type) {
	case int:
		if j, ok := j.(int); ok {
			return i < j
		}
	case float64:
		if j, ok := j.(float64); ok {
			return i < j
		}
	}
	return false
}

// Sort by the values along either axis.
func (df *Pandas) Sort_values(columnName string) {
	columnExists := false
	for _, col := range df.Columns {
		if col == columnName {
			columnExists = true
			break
		}
	}
	if !columnExists {
		panic("Column not found")
	}

	// Obtenez les valeurs de la colonne spécifiée
	values := df.Data[columnName]

	// Créez un slice d'indices pour représenter l'ordre de tri
	indices := make([]int, len(values))
	for i := range indices {
		indices[i] = i
	}

	// Triez les indices basés sur les valeurs
	sort.Slice(indices, func(i, j int) bool {
		return less(values[indices[i]], values[indices[j]])
	})

	// Réarrangez toutes les colonnes basées sur les indices triés
	for colName, colValues := range df.Data {
		newValues := make([]interface{}, len(colValues))
		for i, idx := range indices {
			newValues[i] = df.Data[colName][idx]
		}
		df.Data[colName] = newValues
	}
}

// Select a single column of values.
func (df *Pandas) GetItem(col string) []interface{} {
	return df.Data[col]
}

// Passing a slice ":" selects matching rows.
func (df *Pandas) GetItemSlice(start, end int) *Pandas {
	newData := make(map[string][]interface{})
	for col, values := range df.Data {
		if start < 0 || end > len(values) || start > end {
			panic("index out of range")
		}
		slice := values[start:end]
		newData[col] = make([]interface{}, len(slice))
		copy(newData[col], slice)
	}

	return &Pandas{
		Columns: df.Columns,
		Data:    newData,
	}
}

// Copy the DataFrame
func (df *Pandas) Copy() *Pandas {
	newData := make(map[string][]interface{})
	for col, values := range df.Data {
		newValues := make([]interface{}, len(values))
		copy(newValues, values)
		newData[col] = newValues
	}

	return &Pandas{
		Columns: append([]string(nil), df.Columns...),
		Data:    newData,
	}
}

// Checks if the value is a float64.
func isFloat(val interface{}) bool {
	_, ok := val.(float64)
	return ok
}

// Drops any rows that have missing data.
func (df *Pandas) DropNA() *Pandas {
	newData := make(map[string][]interface{})
	for _, col := range df.Columns {
		newData[col] = make([]interface{}, 0)
	}

	rowCount := len(df.Data[df.Columns[0]])
RowLoop:

	for i := 0; i < rowCount; i++ {
		for _, col := range df.Columns {
			val := df.Data[col][i]
			if val == "nan" || (isFloat(val) && math.IsNaN(val.(float64))) {
				continue RowLoop
			}
		}
		for _, col := range df.Columns {
			newData[col] = append(newData[col], df.Data[col][i])
		}
	}

	return &Pandas{
		Columns: df.Columns,
		Data:    newData,
	}
}

// Checks if the value is a string.
func isMissing(val interface{}) bool {
	switch v := val.(type) {
	case float64:
		return math.IsNaN(v)
	case string:
		return v == "nan" || v == ""
	case nil:
		return true
	default:
		return false
	}
}

// Returns the default value for the type of the value.
func defaultValue(val interface{}) interface{} {
	switch val.(type) {
	case float64:
		return 0.0
	case string:
		return ""
	default:
		return nil
	}
}

// Fills missing data.
func (df *Pandas) FillNA() {
	for col, values := range df.Data {
		for i, val := range values {
			if isMissing(val) {
				df.Data[col][i] = defaultValue(val)
			}
		}
	}
}

// Gets the boolean mask where values are nan.
func (df *Pandas) IsNA() *Pandas {
	newData := make(map[string][]interface{})
	for col, values := range df.Data {
		newValues := make([]interface{}, len(values))
		for i, val := range values {
			newValues[i] = isMissing(val)
		}
		newData[col] = newValues
	}

	return &Pandas{
		Columns: df.Columns,
		Data:    newData,
	}
}

// Get the mean of the values for the requested axis.
func (df *Pandas) Mean() map[string]float64 {
	meanValues := make(map[string]float64)
	for col, values := range df.Data {
		sum := 0.0
		count := 0.0
		for _, val := range values {
			switch v := val.(type) {
			case int, int32, int64, float32, float64:
				var floatValue float64
				switch v := v.(type) {
				case int:
					floatValue = float64(v)
				case int32:
					floatValue = float64(v)
				case int64:
					floatValue = float64(v)
				case float32:
					floatValue = float64(v)
				case float64:
					floatValue = v
				}

				if !math.IsNaN(floatValue) {
					sum += floatValue
					count++
				}
			}
		}
		if count > 0 {
			meanValues[col] = sum / count
		} else {
			meanValues[col] = math.NaN()
		}
	}
	return meanValues
}

// Get the median of the values for the requested axis.
func (df *Pandas) Median() map[string]float64 {
	medianValues := make(map[string]float64)
	for col, values := range df.Data {
		floatValues := make([]float64, 0)
		for _, val := range values {
			if v, ok := val.(float64); ok && !math.IsNaN(v) {
				floatValues = append(floatValues, v)
			}
		}
		sort.Float64s(floatValues)
		n := len(floatValues)
		if n%2 == 0 {
			medianValues[col] = (floatValues[n/2-1] + floatValues[n/2]) / 2
		} else {
			medianValues[col] = floatValues[n/2]
		}
	}
	return medianValues
}

// Get the minimum of the values for the requested axis.
func (df *Pandas) Min() map[string]float64 {
	minValues := make(map[string]float64)
	for col, values := range df.Data {
		min := math.Inf(1)
		for _, val := range values {
			if v, ok := val.(float64); ok && !math.IsNaN(v) && v < min {
				min = v
			}
		}
		if min == math.Inf(1) {
			minValues[col] = math.NaN()
		} else {
			minValues[col] = min
		}
	}
	return minValues
}

// Get the maximum of the values for the requested axis.
func (df *Pandas) Max() map[string]float64 {
	maxValues := make(map[string]float64)
	for col, values := range df.Data {
		max := math.Inf(-1)
		for _, val := range values {
			if v, ok := val.(float64); ok && !math.IsNaN(v) && v > max {
				max = v
			}
		}
		if max == math.Inf(-1) {
			maxValues[col] = math.NaN()
		} else {
			maxValues[col] = max
		}
	}
	return maxValues
}

// Return a Series containing counts of unique values.
func (df *Pandas) Value_counts() map[string]map[interface{}]int {
	valueCounts := make(map[string]map[interface{}]int)
	for col, values := range df.Data {
		valueCounts[col] = make(map[interface{}]int)
		for _, val := range values {
			valueCounts[col][val]++
		}
	}
	return valueCounts
}

// Merge DataFrame or named Series objects with a database-style join.
func (df *Pandas) Merge(right *Pandas, on string) *Pandas {
	mergedData := make(map[string][]interface{})
	for col, values := range df.Data {
		mergedData[col] = values
	}
	for col, values := range right.Data {
		mergedData[col] = values
	}
	return &Pandas{
		Columns: append(df.Columns, right.Columns...),
		Data:    mergedData,
	}
}

// Concatenate pandas objects along a particular axis with optional set logic along the other axes.
func (df *Pandas) Concat(right *Pandas) *Pandas {
	concatData := make(map[string][]interface{})
	for col, values := range df.Data {
		concatData[col] = values
	}
	for col, values := range right.Data {
		concatData[col] = append(concatData[col], values...)
	}
	return &Pandas{
		Columns: df.Columns,
		Data:    concatData,
	}
}

// Group DataFrame using a mapper or by a Series of columns.
func (df *Pandas) GroupBy(by string) map[interface{}]*Pandas {
	groups := make(map[interface{}]*Pandas)
	for i, val := range df.Data[by] {
		if _, ok := groups[val]; !ok {
			groups[val] = &Pandas{
				Columns: df.Columns,
				Data:    make(map[string][]interface{}),
			}
		}
		for col, values := range df.Data {
			groups[val].Data[col] = append(groups[val].Data[col], values[i])
		}
	}
	return groups
}

// Compute sum of group values.
func (df *Pandas) Sum() map[string]float64 {
	sumValues := make(map[string]float64)
	for col, values := range df.Data {
		sum := 0.0
		for _, val := range values {
			if v, ok := val.(float64); ok && !math.IsNaN(v) {
				sum += v
			}
		}
		sumValues[col] = sum
	}
	return sumValues
}