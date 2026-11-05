package graal.systems.examples.beam;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Word count with Apache Beam: counts the words of an embedded text, prints the most frequent ones,
 * and checks the pipeline's totals against a plain Java count of the same text.
 *
 * <p>graal adds the runner options ({@code --runner=DirectRunner --targetParallelism=N}); the job's
 * parameters come after them, for example {@code --top=5}.
 */
public final class WordCount {

    /** Opening lines of "A Tale of Two Cities" (Charles Dickens, 1859, public domain). */
    static final List<String> TEXT = List.of(
            "It was the best of times, it was the worst of times,",
            "it was the age of wisdom, it was the age of foolishness,",
            "it was the epoch of belief, it was the epoch of incredulity,",
            "it was the season of Light, it was the season of Darkness,",
            "it was the spring of hope, it was the winter of despair,",
            "we had everything before us, we had nothing before us,",
            "we were all going direct to Heaven, we were all going direct the other way.");

    /** Pipeline options of this example; Beam reads them from the program arguments. */
    public interface Options extends PipelineOptions {
        @Description("Number of words to print")
        @Default.Integer(10)
        int getTop();

        void setTop(int top);
    }

    static List<String> words(String line) {
        List<String> words = new ArrayList<>();
        for (String word : line.toLowerCase(Locale.ROOT).split("[^\\p{L}']+")) {
            if (!word.isEmpty()) {
                words.add(word);
            }
        }
        return words;
    }

    /** Prints the top words; runs once, on the single list produced by {@link Top}. */
    static final class PrintTop extends DoFn<List<KV<String, Long>>, Void> {
        @ProcessElement
        public void process(@Element List<KV<String, Long>> top) {
            System.out.println("Top " + top.size() + " words:");
            for (KV<String, Long> kv : top) {
                System.out.printf(Locale.ROOT, "  %-12s %3d%n", kv.getKey(), kv.getValue());
            }
        }
    }

    /** Counts words and distinct words in Beam metrics, read back by the driver. */
    static final class Tally extends DoFn<KV<String, Long>, KV<String, Long>> {
        private final Counter words = Metrics.counter(WordCount.class, "words");
        private final Counter distinct = Metrics.counter(WordCount.class, "distinct");

        @ProcessElement
        public void process(@Element KV<String, Long> kv, OutputReceiver<KV<String, Long>> out) {
            words.inc(kv.getValue());
            distinct.inc();
            out.output(kv);
        }
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Text", Create.of(TEXT))
                .apply("Words", FlatMapElements.into(TypeDescriptors.strings()).via(WordCount::words))
                .apply("Count", Count.perElement())
                .apply("Tally", ParDo.of(new Tally()))
                .apply("Top", Top.of(options.getTop(), new ByCount()))
                .apply("Print", ParDo.of(new PrintTop()));

        PipelineResult result = pipeline.run();
        PipelineResult.State state = result.waitUntilFinish();
        if (state != PipelineResult.State.DONE) {
            System.err.println("Pipeline ended in state " + state);
            System.exit(2);
        }

        long words = counter(result, "words");
        long distinct = counter(result, "distinct");
        Map<String, Long> expected = new HashMap<>();
        TEXT.stream().flatMap(line -> words(line).stream()).forEach(w -> expected.merge(w, 1L, Long::sum));
        long expectedWords = expected.values().stream().mapToLong(Long::longValue).sum();
        System.out.printf(Locale.ROOT, "Counted %d words, %d distinct (expected %d and %d)%n",
                words, distinct, expectedWords, expected.size());
        if (words != expectedWords || distinct != expected.size()) {
            System.err.println("Word count mismatch");
            System.exit(3);
        }
        System.out.println("Word count OK");
    }

    static long counter(PipelineResult result, String name) {
        for (MetricResult<Long> counter : result.metrics()
                .queryMetrics(MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.named(WordCount.class, name)).build())
                .getCounters()) {
            return counter.getAttempted();
        }
        return -1;
    }

    /** Orders by count, then alphabetically (reverse, so that Top keeps "a" before "b" on ties). */
    static final class ByCount implements Comparator<KV<String, Long>>, Serializable {
        @Override
        public int compare(KV<String, Long> a, KV<String, Long> b) {
            int byCount = Long.compare(a.getValue(), b.getValue());
            return byCount != 0 ? byCount : b.getKey().compareTo(a.getKey());
        }
    }

    private WordCount() {
    }
}
