package com.bms.bmsdatapublisher.batch;

import com.bms.bmsdatapublisher.domain.BatteryRecord;
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
public class BatteryDataPublishConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;


    public BatteryDataPublishConfiguration(JobBuilderFactory jobBuilderFactory,
                                           StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public Job batteryJob() throws Exception {
        return jobBuilderFactory.get("batteryJob")
                .incrementer(new RunIdIncrementer())
                .start(this.batteryStep())
                .build();
    }

    @Bean
    public Step batteryStep() throws Exception {
        return stepBuilderFactory.get("batteryStep")
                .<BatteryRecord,BatteryRecord>chunk(1)
                .reader(csvBatteryItemReader())
                .processor(batteryItemProcessor())
                .writer(jdbcBatchBatteryItemWriter())
                .build();
    }


    private ItemReader<BatteryRecord> csvBatteryItemReader() throws Exception {
        DefaultLineMapper<BatteryRecord> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        lineMapper.setLineTokenizer(tokenizer);

        lineMapper.setFieldSetMapper(fieldSet -> {
            log.info("battery record, {}", fieldSet);
            String carId = fieldSet.readString(0);
            Double stateOfChrgBms = fieldSet.readDouble(1);
            Double stateOfChrgDisp = fieldSet.readDouble(2);
            Integer rapidChrgPort = fieldSet.readInt(3);
            Integer normalChrgPort = fieldSet.readInt(4);
            Double stateOfHealth = fieldSet.readDouble(5);
            Double btryMinTempr = fieldSet.readDouble(6);
            Double btryMaxTempr = fieldSet.readDouble(7);
            Double minCellVolt = fieldSet.readDouble(8);
            Double maxCellVolt = fieldSet.readDouble(9);
            String btryCellsArr = fieldSet.readString(10);
            String btryMdulTemprArr = fieldSet.readString(11);
            Double mvmnTime = fieldSet.readDouble(12);

            return new BatteryRecord(carId, stateOfChrgBms, stateOfChrgDisp, rapidChrgPort, normalChrgPort,
                    stateOfHealth, btryMinTempr, btryMaxTempr, minCellVolt, maxCellVolt, btryCellsArr, btryMdulTemprArr, mvmnTime);
        });
        FlatFileItemReader itemReader = new FlatFileItemReaderBuilder<BatteryRecord>()
                .name("cvsItemReader")
                .encoding("UTF-8")
                .resource(new ClassPathResource("battery_record.csv"))
                .linesToSkip(1)
                .lineMapper(lineMapper)
                .build();
        itemReader.afterPropertiesSet();

        return itemReader;
    }

    private ItemProcessor<BatteryRecord, BatteryRecord> batteryItemProcessor() {
       return item -> {
           Thread.sleep(1000);

           item.setColecTime(LocalDateTime.now());
           return item;
       };
    }


    private ItemWriter<BatteryRecord> jdbcBatchBatteryItemWriter() {

        JdbcBatchItemWriter<BatteryRecord> itemWriter = new JdbcBatchItemWriterBuilder<BatteryRecord>()
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("insert into battery_record(car_id, colec_time, state_of_chrg_bms, " +
                        "state_of_chrg_disp, rapid_chrg_port,normal_chrg_port, state_of_health, " +
                        "btry_min_tempr,btry_max_tempr, min_cell_volt, max_cell_volt, btry_cells_arr," +
                        "btry_mdul_tempr_arr,mvmn_time ) " +
                        "values (:carId, :colecTime, :stateOfChrgBms, :stateOfChrgDisp,:rapidChrgPort, :normalChrgPort,  " +
                        ":stateOfHealth, :btryMinTempr, :btryMaxTempr, :minCellVolt, :maxCellVolt, :btryCellsArr," +
                        ":btryMdulTemprArr, :mvmnTime)")
                .build();
        itemWriter.afterPropertiesSet();

        return itemWriter;
    }


}
