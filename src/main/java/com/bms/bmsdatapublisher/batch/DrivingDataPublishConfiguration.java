package com.bms.bmsdatapublisher.batch;

import com.bms.bmsdatapublisher.domain.DrivingRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;
import java.time.LocalDateTime;

@Configuration
@Slf4j
public class DrivingDataPublishConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;


    public DrivingDataPublishConfiguration(JobBuilderFactory jobBuilderFactory,
                                           StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public Job drivingJob() throws Exception {
        return jobBuilderFactory.get("drivingJob")
                .incrementer(new RunIdIncrementer())
                .start(this.mvmnStep())
                .build();
    }


    @Bean
    public Step mvmnStep() throws Exception {
        return stepBuilderFactory.get("mvmnStep")
                .<DrivingRecord, DrivingRecord>chunk(1)
                .reader(csvMvmnItemReader())
                .processor(mvmnItemProcessor())
                .writer(jdbcBatchMvmnItemWriter())
                .build();
    }

    private ItemReader<DrivingRecord> csvMvmnItemReader() throws Exception {
        DefaultLineMapper<DrivingRecord> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        lineMapper.setLineTokenizer(tokenizer);

        lineMapper.setFieldSetMapper(fieldSet -> {
            log.info("driving record, {}", fieldSet);
            String carId = fieldSet.readString(0);
            Integer odometer = fieldSet.readInt(1);
            Double socReal = fieldSet.readDouble(2);
            Double socDisplay = fieldSet.readDouble(3);
            Double vehicleSpeed = fieldSet.readDouble(4);
            Integer chargeStatus = fieldSet.readInt(5);
            Integer keyStatus = fieldSet.readInt(6);
            Double xcord = fieldSet.readDouble(7);
            Double ycord = fieldSet.readDouble(8);


            return new DrivingRecord(carId, odometer, socReal, socDisplay, vehicleSpeed,
                    chargeStatus, keyStatus, xcord, ycord);
        });

        FlatFileItemReader csvMvmnItemReader = new FlatFileItemReaderBuilder<DrivingRecord>()
                .name("csvMvmnItemReader")
                .encoding("UTF-8")
                .resource(new ClassPathResource("driving_record.csv"))
                .linesToSkip(1)
                .lineMapper(lineMapper)
                .build();
        csvMvmnItemReader.afterPropertiesSet();

        return csvMvmnItemReader;
    }

    private ItemProcessor<DrivingRecord, DrivingRecord> mvmnItemProcessor() {
        return item -> {
            Thread.sleep(1000);

            item.setUpdatedTime(LocalDateTime.now());
            return item;
        };
    }

    private ItemWriter<DrivingRecord> jdbcBatchMvmnItemWriter() {

        JdbcBatchItemWriter<DrivingRecord> jdbcBatchMvmnItemWriter = new JdbcBatchItemWriterBuilder<DrivingRecord>()
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("insert into driving_record(car_id, charge_status, key_status, " +
                        "odometer, soc_display , soc_real, updated_time, " +
                        "vehicle_speed , xcord, ycord) " +
                        "values (:carId, :chargeStatus, :keyStatus, :odometer,:socDisplay, :socReal,  " +
                        ":updatedTime, :vehicleSpeed, :xcord, :ycord)")
                .build();
        jdbcBatchMvmnItemWriter.afterPropertiesSet();

        return jdbcBatchMvmnItemWriter;
    }
}