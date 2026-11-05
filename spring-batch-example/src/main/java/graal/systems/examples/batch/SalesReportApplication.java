package graal.systems.examples.batch;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * A two-step Spring Batch job on the sales embedded in the jar ({@code sales.csv}):
 *
 * <ol>
 *   <li>{@code load}: reads the CSV in chunks, rejects invalid rows (no price, quantity not positive),
 *       computes each line's amount and inserts it into the {@code sale} table of an in-memory H2;</li>
 *   <li>{@code report}: prints the revenue per store and per day, and fails the job when more than
 *       {@code sales.max-rejected} rows were rejected.</li>
 * </ol>
 *
 * The process exits with the job's outcome: 0 when it completes, 1 when it fails.
 */
@SpringBootApplication
public class SalesReportApplication {

    /** One line of the CSV, as read. */
    public record SaleLine(String date, String store, String product, String quantity, String unitPrice) {
    }

    /** One validated sale, as stored. */
    public record Sale(LocalDate date, String store, String product, int quantity, long amountCents) {
    }

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(SalesReportApplication.class, args)));
    }

    @Bean
    FlatFileItemReader<SaleLine> salesReader() {
        return new FlatFileItemReaderBuilder<SaleLine>()
                .name("salesReader")
                .resource(new ClassPathResource("sales.csv"))
                .linesToSkip(1)
                .delimited()
                .names("date", "store", "product", "quantity", "unitPrice")
                .targetType(SaleLine.class)
                .build();
    }

    /** Returns null for an invalid row: Spring Batch counts it as filtered and does not write it. */
    @Bean
    ItemProcessor<SaleLine, Sale> validator() {
        return line -> {
            if (line.unitPrice() == null || line.unitPrice().isBlank()) {
                System.out.println("Rejected (no price): " + line);
                return null;
            }
            int quantity = Integer.parseInt(line.quantity().trim());
            if (quantity <= 0) {
                System.out.println("Rejected (quantity " + quantity + "): " + line);
                return null;
            }
            long amountCents = new BigDecimal(line.unitPrice().trim())
                    .multiply(BigDecimal.valueOf(quantity))
                    .movePointRight(2)
                    .setScale(0, RoundingMode.HALF_UP)
                    .longValueExact();
            return new Sale(LocalDate.parse(line.date().trim()), line.store().trim(), line.product().trim(),
                    quantity, amountCents);
        };
    }

    @Bean
    JdbcBatchItemWriter<Sale> salesWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Sale>()
                .dataSource(dataSource)
                .sql("INSERT INTO sale (sale_date, store, product, quantity, amount_cents) VALUES (?, ?, ?, ?, ?)")
                .itemPreparedStatementSetter((sale, ps) -> {
                    ps.setDate(1, Date.valueOf(sale.date()));
                    ps.setString(2, sale.store());
                    ps.setString(3, sale.product());
                    ps.setInt(4, sale.quantity());
                    ps.setLong(5, sale.amountCents());
                })
                .build();
    }

    @Bean
    Step load(JobRepository repository, PlatformTransactionManager transactionManager,
              FlatFileItemReader<SaleLine> salesReader, ItemProcessor<SaleLine, Sale> validator,
              JdbcBatchItemWriter<Sale> salesWriter) {
        return new StepBuilder("load", repository)
                .<SaleLine, Sale>chunk(5)
                .transactionManager(transactionManager)
                .reader(salesReader)
                .processor(validator)
                .writer(salesWriter)
                .build();
    }

    @Bean
    Step report(JobRepository repository, PlatformTransactionManager transactionManager, JdbcTemplate jdbc,
                @Value("${sales.max-rejected}") int maxRejected) {
        return new StepBuilder("report", repository).tasklet((contribution, context) -> {
            var load = context.getStepContext().getStepExecution().getJobExecution().getStepExecutions().stream()
                    .filter(step -> step.getStepName().equals("load"))
                    .findFirst()
                    .orElseThrow();
            System.out.printf(Locale.ROOT, "Read %d rows, loaded %d, rejected %d%n",
                    load.getReadCount(), load.getWriteCount(), load.getFilterCount());

            System.out.println("Revenue per store:");
            List<Map<String, Object>> stores = jdbc.queryForList(
                    "SELECT store, COUNT(*) AS lines, SUM(quantity) AS units, SUM(amount_cents) AS cents"
                            + " FROM sale GROUP BY store ORDER BY cents DESC");
            for (Map<String, Object> row : stores) {
                System.out.printf(Locale.ROOT, "  %-8s %3d lines %5d units %10s EUR%n",
                        row.get("STORE"), row.get("LINES"), row.get("UNITS"), euros(row.get("CENTS")));
            }
            System.out.println("Revenue per day:");
            for (Map<String, Object> row : jdbc.queryForList(
                    "SELECT sale_date, SUM(amount_cents) AS cents FROM sale GROUP BY sale_date ORDER BY sale_date")) {
                System.out.printf(Locale.ROOT, "  %s %10s EUR%n", row.get("SALE_DATE"), euros(row.get("CENTS")));
            }
            Long total = jdbc.queryForObject("SELECT SUM(amount_cents) FROM sale", Long.class);
            System.out.printf(Locale.ROOT, "Total revenue: %s EUR%n", euros(total));

            if (stores.isEmpty()) {
                throw new IllegalStateException("no sale loaded");
            }
            if (load.getFilterCount() > maxRejected) {
                throw new IllegalStateException(load.getFilterCount() + " rows rejected, more than sales.max-rejected="
                        + maxRejected);
            }
            return RepeatStatus.FINISHED;
        }, transactionManager).build();
    }

    @Bean
    Job salesReport(JobRepository repository, Step load, Step report) {
        return new JobBuilder("sales-report", repository).start(load).next(report).build();
    }

    static String euros(Object cents) {
        return BigDecimal.valueOf(((Number) cents).longValue(), 2).toPlainString();
    }
}
