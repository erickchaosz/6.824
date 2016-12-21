package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	// Dict that contains Key of KeyValue as key
	// Used to temporarily hold the keyvalues from the intermediate files
	interDict := make(map[string][]string)

	for m := 0; m < nMap; m++ {
		inFile := reduceName(jobName, m, reduceTaskNumber)

		f, err := os.Open(inFile)
		if err != nil {
			panic(err)
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			interDict[kv.Key] = append(interDict[kv.Key], kv.Value)
		}
		f.Close()
	}

	// Now go through the dict in sorted key order and saves reduced result
	var keys []string
	for key := range interDict {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	mergeFile := mergeName(jobName, reduceTaskNumber)
	f, err := os.Create(mergeFile)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	enc := json.NewEncoder(f)
	for _, key := range keys {

		enc.Encode(KeyValue{key, reduceF(key, interDict[key])})
	}

}
