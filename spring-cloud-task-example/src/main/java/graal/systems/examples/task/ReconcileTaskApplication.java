package graal.systems.examples.task;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.cloud.task.listener.TaskExecutionListener;
import org.springframework.cloud.task.repository.TaskExecution;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;

/**
 * A Spring Cloud Task that reconciles the payments received against the invoices issued (both
 * embedded in the jar). Each invoice ends up paid, paid with a difference, or unpaid; each payment
 * that matches no invoice is reported as unmatched.
 *
 * <p>Spring Cloud Task records the execution (name, start, end, exit code) in its task repository,
 * here an in-memory H2. graal sets the task name from the job's {@code task_name} option. Program
 * arguments are passed to the task: {@code --tolerance-cents=N} overrides the tolerance.
 */
@SpringBootApplication
@EnableTask
public class ReconcileTaskApplication {

    record Invoice(String id, String customer, long cents) {
    }

    record Payment(String id, String invoice, long cents) {
    }

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(ReconcileTaskApplication.class, args)));
    }

    @Bean
    ApplicationRunner reconcile(@Value("${reconcile.tolerance-cents}") long defaultTolerance) {
        return args -> {
            long tolerance = args.containsOption("tolerance-cents")
                    ? Long.parseLong(args.getOptionValues("tolerance-cents").getFirst())
                    : defaultTolerance;
            Map<String, Invoice> invoices = new LinkedHashMap<>();
            for (String[] f : csv("invoices.csv")) {
                invoices.put(f[0], new Invoice(f[0], f[1], cents(f[2])));
            }
            Map<String, Long> paid = new LinkedHashMap<>();
            List<Payment> unmatched = new ArrayList<>();
            for (String[] f : csv("payments.csv")) {
                Payment payment = new Payment(f[0], f[1], cents(f[2]));
                if (invoices.containsKey(payment.invoice())) {
                    paid.merge(payment.invoice(), payment.cents(), Long::sum);
                } else {
                    unmatched.add(payment);
                }
            }

            int ok = 0;
            int differences = 0;
            int unpaid = 0;
            long outstanding = 0;
            System.out.printf(Locale.ROOT, "Reconciling %d invoices and %d payments (tolerance %s EUR)%n",
                    invoices.size(), paid.size() + unmatched.size(), euros(tolerance));
            for (Invoice invoice : invoices.values()) {
                Long received = paid.get(invoice.id());
                if (received == null) {
                    unpaid++;
                    outstanding += invoice.cents();
                    System.out.printf(Locale.ROOT, "  UNPAID     %s %-9s %10s EUR%n",
                            invoice.id(), invoice.customer(), euros(invoice.cents()));
                } else if (Math.abs(invoice.cents() - received) > tolerance) {
                    differences++;
                    outstanding += invoice.cents() - received;
                    System.out.printf(Locale.ROOT, "  DIFFERENCE %s %-9s %10s EUR invoiced, %10s EUR paid%n",
                            invoice.id(), invoice.customer(), euros(invoice.cents()), euros(received));
                } else {
                    ok++;
                }
            }
            for (Payment payment : unmatched) {
                System.out.printf(Locale.ROOT, "  UNMATCHED  %s for unknown invoice %s, %s EUR%n",
                        payment.id(), payment.invoice(), euros(payment.cents()));
            }
            System.out.printf(Locale.ROOT,
                    "Reconciled: %d paid, %d with a difference, %d unpaid, %d unmatched payment(s); outstanding %s EUR%n",
                    ok, differences, unpaid, unmatched.size(), euros(outstanding));
            if (ok + differences + unpaid != invoices.size()) {
                throw new IllegalStateException("every invoice must be classified once");
            }
        };
    }

    /** Prints the execution that Spring Cloud Task opens and closes around the run. */
    @Bean
    TaskExecutionListener executionLog() {
        return new TaskExecutionListener() {
            @Override
            public void onTaskStartup(TaskExecution execution) {
                System.out.println("Task '" + execution.getTaskName() + "' execution " + execution.getExecutionId()
                        + " started, arguments " + execution.getArguments());
            }

            @Override
            public void onTaskEnd(TaskExecution execution) {
                System.out.println("Task '" + execution.getTaskName() + "' execution " + execution.getExecutionId()
                        + " ended with exit code " + execution.getExitCode());
            }

            @Override
            public void onTaskFailed(TaskExecution execution, Throwable throwable) {
                System.err.println("Task '" + execution.getTaskName() + "' failed: " + throwable);
            }
        };
    }

    static List<String[]> csv(String resource) {
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new ClassPathResource(resource).getInputStream(), StandardCharsets.UTF_8))) {
            reader.readLine(); // header
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (!line.isBlank()) {
                    rows.add(line.split(",", -1));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return rows;
    }

    static long cents(String amount) {
        return new BigDecimal(amount.trim()).movePointRight(2).longValueExact();
    }

    static String euros(long cents) {
        return BigDecimal.valueOf(cents, 2).toPlainString();
    }
}
