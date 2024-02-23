package main

import (
	"fmt"
	"github.com/GuillaumeDupuy/pandas"
	"os"
	"math"
)

func main() {
	df, err := pandas.ReadCSV("data.csv")
	if err != nil {
		fmt.Println("Error reading CSV:", err)
		os.Exit(1)
	}

	df.Head(5)

	df.Shape()

	fmt.Println("--------------------")

	df2 := pandas.NewDataFrame([]string{"a", "b", "c"}, map[string][]interface{}{
		"a": {1, 2, 3},
		"b": {4, 5, 6},
		"c": {7, 8, 9},
	})

	df2.Head(5)

	df2.Describe()

	df2.Dtypes()

	fmt.Println("--------------------")

	df3 := pandas.NewDataFrame([]string{"a","b","c"}, map[string][]interface{}{
		"a": {"hello", math.NaN(), "pandas"},
		"b": {1, 2, math.NaN()},
		"c": {47, 3, 4},
	})

	fmt.Println("before dropna")
	df3.Head(5)

	dfclean := df3.DropNA()

	fmt.Println("after dropna")
	dfclean.Head(5)

	fmt.Println("--------------------")

	dfcopy := df3.Copy()

	fmt.Println("before fillna")
	dfcopy.Head(5)

	dfcopy.FillNA()

	fmt.Println("after fillna")
	dfcopy.Head(5)

	fmt.Println("--------------------")

	dfcopy2 := df3.Copy()

	fmt.Println("before isna")
	dfcopy2.Head(5)

	dfclean3 := dfcopy2.IsNA()

	fmt.Println("after isna")
	dfclean3.Head(5)

	fmt.Println("--------------------")

	fmt.Println("mean")

	fmt.Println(df3.Mean())

	fmt.Println("--------------------")

	df4 := pandas.NewDataFrame([]string{"a", "b", "c"}, map[string][]interface{}{
		"a": {3, 1, 2},
		"b": {4, 5, 6},
		"c": {7, 8, 9},
	})

	df5 := df4.Copy()

	fmt.Println("before sort_values")
	df4.Head(5)

	df4.Sort_values("a")

	fmt.Println("after sort_values")
	df4.Head(5)

	fmt.Println("--------------------")

	fmt.Println("before sort_index")
	df5.Head(5)

	df5.Sort_index()

	fmt.Println("after sort_index")
	df5.Head(5)

	fmt.Println("--------------------")

	df6, err := pandas.ReadTXT("data.txt")
	if err != nil {
		fmt.Println("Error reading CSV:", err)
		os.Exit(1)
	}

	df6.Head(5)

	// fmt.Println("--------------------")

	// Not working
	// fmt.Println(df6.Median())

	// fmt.Println("--------------------")

	// Not working
	// fmt.Println(df6.Min())

	// fmt.Println("--------------------")

	// Not working
	// fmt.Println(df6.Max())

	// fmt.Println("--------------------")

	// Not working
	// fmt.Println(df6.Sum())

	fmt.Println("--------------------")

	fmt.Println(df6.Value_counts())

	// fmt.Println("--------------------")

	// Not working
	// fmt.Println(df6.GroupBy("age"))
}
