package graal.systems.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The same job, run by the local job runner on local files: no cluster, no bridge. */
class ExampleRunnerTest {

    @Test
    void countsWordsLocally(@TempDir Path dir) throws Exception {
        Path input = dir.resolve("input.txt");
        Files.writeString(input, "graal hadoop graal\nyarn graal\n");
        Path output = dir.resolve("output");

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        int rc = ToolRunner.run(conf, new ExampleRunner(), new String[]{input.toUri().toString(), output.toUri().toString()});

        assertThat(rc).isZero();
        List<String> lines = Files.readAllLines(output.resolve("part-00000"));
        assertThat(lines).containsExactly("graal\t3", "hadoop\t1", "yarn\t1");
    }
}
