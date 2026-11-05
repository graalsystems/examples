package graal.systems.examples.pekko;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;

/**
 * An Apache Pekko Streams pipeline over generated sensor readings: drops the invalid ones, groups
 * the others by sensor, and computes count, min, mean and max per sensor in parallel sub-streams.
 * The totals are checked against a plain Java pass over the same readings.
 *
 * <p>Arguments: the number of readings to generate (default 100000) and the random seed (default 42).
 */
public final class SensorStats {

    static final String[] SENSORS = {"boiler", "cellar", "kitchen", "roof", "server-room"};

    /** A temperature reading; NaN when the sensor failed to answer. */
    record Reading(String sensor, double celsius) {
        boolean valid() {
            return !Double.isNaN(celsius);
        }
    }

    /** Running statistics of one sensor. */
    record Stats(String sensor, long count, double min, double max, double sum) {
        static Stats empty() {
            return new Stats(null, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0);
        }

        Stats add(Reading r) {
            return new Stats(r.sensor(), count + 1, Math.min(min, r.celsius()), Math.max(max, r.celsius()),
                    sum + r.celsius());
        }

        double mean() {
            return sum / count;
        }
    }

    static List<Reading> generate(int count, long seed) {
        Random random = new Random(seed);
        double[] base = {65.0, 12.0, 21.0, 8.0, 27.0};
        List<Reading> readings = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int s = random.nextInt(SENSORS.length);
            double value = random.nextInt(100) == 0 ? Double.NaN : base[s] + random.nextGaussian() * 2.5;
            readings.add(new Reading(SENSORS[s], value));
        }
        return readings;
    }

    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 100_000;
        long seed = args.length > 1 ? Long.parseLong(args[1]) : 42L;
        List<Reading> readings = generate(count, seed);

        ActorSystem system = ActorSystem.create("sensor-stats");
        int status = 0;
        try {
            List<Stats> stats = Source.from(readings)
                    .filter(Reading::valid)
                    .groupBy(SENSORS.length, Reading::sensor)
                    .fold(Stats.empty(), Stats::add)
                    .async()
                    .mergeSubstreams()
                    .runWith(Sink.seq(), system)
                    .toCompletableFuture()
                    .get(2, TimeUnit.MINUTES);

            List<Stats> sorted = new ArrayList<>(stats);
            sorted.sort(Comparator.comparing(Stats::sensor));
            System.out.printf(Locale.ROOT, "%d readings generated (seed %d)%n", count, seed);
            System.out.printf(Locale.ROOT, "  %-12s %7s %8s %8s %8s%n", "sensor", "count", "min", "mean", "max");
            long total = 0;
            for (Stats s : sorted) {
                System.out.printf(Locale.ROOT, "  %-12s %7d %8.2f %8.2f %8.2f%n",
                        s.sensor(), s.count(), s.min(), s.mean(), s.max());
                total += s.count();
            }

            long expected = readings.stream().filter(Reading::valid).count();
            System.out.printf(Locale.ROOT, "%d valid readings in %d sensors, %d dropped (expected %d valid)%n",
                    total, sorted.size(), count - total, expected);
            if (total != expected || sorted.size() != SENSORS.length) {
                System.err.println("Statistics do not match the input");
                status = 3;
            } else {
                System.out.println("Sensor statistics OK");
            }
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(30, TimeUnit.SECONDS);
        }
        System.exit(status);
    }

    private SensorStats() {
    }
}
