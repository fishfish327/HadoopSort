package mySort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MySortMapper extends Mapper<Object, Text, Text, Text>{
    private Text inputKey = new Text();
    private Text inputValue = new Text();

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String input = value.toString();
        String[] lines = input.split("\n");

        // get one record per line
        for (String str : lines) {
            inputKey.set(key.toString().substring(0, 10));
            inputValue.set(key.toString().substring(10, 98));
            context.write(inputKey, inputValue);
        }
    }
}
