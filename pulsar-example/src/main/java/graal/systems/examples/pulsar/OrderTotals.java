package graal.systems.examples.pulsar;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

/**
 * A small streaming pipeline on Apache Pulsar, against the standalone broker that graal starts in
 * the run's pod:
 *
 * <ol>
 *   <li>a producer publishes order events to the orders topic, keyed by store;</li>
 *   <li>a consumer reads them, acknowledges each one and sums the revenue per store;</li>
 *   <li>the totals are published to the totals topic, then read back and checked.</li>
 * </ol>
 *
 * <p>Arguments: the orders topic and the totals topic (default {@code persistent://public/default/orders}
 * and {@code persistent://public/default/order-totals}). The broker URL comes from
 * {@code PULSAR_SERVICE_URL}, set by graal.
 */
public final class OrderTotals {

    /** Order events: store, product, quantity, unit price. */
    static final List<String> ORDERS = List.of(
            "lyon,coffee,2,4.50", "paris,croissant,6,1.40", "nantes,tea,1,3.20", "paris,coffee,3,4.80",
            "lyon,croissant,4,1.30", "paris,tea,2,3.40", "nantes,coffee,5,4.50", "lyon,tea,3,3.20",
            "paris,coffee,1,4.80", "nantes,croissant,12,1.30", "lyon,coffee,6,4.50", "paris,juice,2,3.90",
            "nantes,coffee,2,4.50", "lyon,croissant,10,1.30", "paris,croissant,8,1.40", "lyon,tea,1,3.20");

    public static void main(String[] args) throws Exception {
        String serviceUrl = System.getenv().getOrDefault("PULSAR_SERVICE_URL",
                System.getProperty("pulsar.serviceUrl", "pulsar://127.0.0.1:6650"));
        String ordersTopic = args.length > 0 ? args[0] : "persistent://public/default/orders";
        String totalsTopic = args.length > 1 ? args[1] : "persistent://public/default/order-totals";
        System.out.println("Broker " + serviceUrl + ", orders " + ordersTopic + ", totals " + totalsTopic);

        Map<String, BigDecimal> expected = new TreeMap<>();
        for (String order : ORDERS) {
            String[] f = order.split(",");
            expected.merge(f[0], amount(f), BigDecimal::add);
        }

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build()) {
            // Subscribe first, from the earliest message: nothing published below can be missed.
            try (Consumer<String> orders = client.newConsumer(Schema.STRING).topic(ordersTopic)
                    .subscriptionName("order-totals")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
                 Producer<String> producer = client.newProducer(Schema.STRING).topic(ordersTopic).create()) {
                for (String order : ORDERS) {
                    producer.newMessage().key(order.substring(0, order.indexOf(','))).value(order).send();
                }
                System.out.println("Published " + ORDERS.size() + " orders");

                Map<String, BigDecimal> totals = new TreeMap<>();
                for (int i = 0; i < ORDERS.size(); i++) {
                    Message<String> message = orders.receive(30, TimeUnit.SECONDS);
                    if (message == null) {
                        fail("only " + i + " of " + ORDERS.size() + " orders received");
                    }
                    String[] f = message.getValue().split(",");
                    totals.merge(f[0], amount(f), BigDecimal::add);
                    orders.acknowledge(message);
                }
                System.out.println("Consumed " + ORDERS.size() + " orders");

                try (Producer<String> out = client.newProducer(Schema.STRING).topic(totalsTopic).create()) {
                    for (Map.Entry<String, BigDecimal> total : totals.entrySet()) {
                        out.newMessage().key(total.getKey()).value(total.getKey() + "," + total.getValue()).send();
                    }
                }
            }

            // Read the totals back, as any downstream consumer would.
            try (Consumer<String> totals = client.newConsumer(Schema.STRING).topic(totalsTopic)
                    .subscriptionName("check")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
                Map<String, BigDecimal> read = new TreeMap<>();
                System.out.println("Revenue per store (from " + totalsTopic + "):");
                for (int i = 0; i < expected.size(); i++) {
                    Message<String> message = totals.receive(30, TimeUnit.SECONDS);
                    if (message == null) {
                        fail("only " + i + " of " + expected.size() + " totals read back");
                    }
                    String[] f = message.getValue().split(",");
                    read.put(f[0], new BigDecimal(f[1]));
                    totals.acknowledge(message);
                    System.out.printf(Locale.ROOT, "  %-8s %8s EUR%n", f[0], f[1]);
                }
                if (!read.equals(expected)) {
                    fail("totals " + read + " differ from the expected " + expected);
                }
            }
        }
        BigDecimal sum = expected.values().stream().reduce(BigDecimal.ZERO, BigDecimal::add);
        System.out.println("Order totals OK: " + ORDERS.size() + " orders, " + sum + " EUR");
    }

    static BigDecimal amount(String[] order) {
        return new BigDecimal(order[3]).multiply(new BigDecimal(order[2]));
    }

    static void fail(String reason) {
        System.err.println("Failed: " + reason);
        System.exit(3);
    }

    private OrderTotals() {
    }
}
