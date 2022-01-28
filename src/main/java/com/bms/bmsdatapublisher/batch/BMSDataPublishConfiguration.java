package com.bms.bmsdatapublisher.batch;

import com.bms.bmsdatapublisher.domain.BatteryRecord;
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
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;

@Configuration
@Slf4j
public class BMSDataPublishConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;


    public BMSDataPublishConfiguration(JobBuilderFactory jobBuilderFactory,
                                       StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public Job bmsJob() throws Exception {
        return jobBuilderFactory.get("bmsJob")
                .incrementer(new RunIdIncrementer())
                .start(this.batteryStep())
                .next(this.mvmnStep())
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

    @Bean
    public Step mvmnStep() throws Exception {
        return stepBuilderFactory.get("mvmnStep")
                .<DrivingRecord, DrivingRecord>chunk(1)
                .reader(csvMvmnItemReader())
                .processor(mvmnItemProcessor())
                .writer(jdbcBatchMvmnItemWriter())
                .build();
    }

    private ItemReader<BatteryRecord> csvBatteryItemReader() throws Exception {
        DefaultLineMapper<BatteryRecord> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
//        tokenizer.setNames("carId", "stateOfChrgBms", "stateOfChrgDisp", "rapidChrgPort", "normalChrgPort",
//                "stateOfHealth", "btryMinTempr", "btryMaxTempr", "minCellVolt", "maxCellVolt", "btryCellsArr", "btryMdulTemprArr","mvmnTime");
        lineMapper.setLineTokenizer(tokenizer);

        lineMapper.setFieldSetMapper(fieldSet -> {
            log.info("fieldSet, {}", fieldSet);
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
                .resource(new ClassPathResource("batteryRecord.csv"))
//                .linesToSkip(1)
                .lineMapper(lineMapper)
                .build();
        itemReader.afterPropertiesSet();

        return itemReader;
    }

    private ItemProcessor<BatteryRecord, BatteryRecord> batteryItemProcessor() {
       return item -> {
           Thread.sleep(1000);

           item.setColecTime(LocalDateTime.now());
           log.info("processor: {}", item);
           return item;
       };
    }

    private ItemWriter<BatteryRecord> batteryItemWriter (){
        return items -> {
            log.info("items,{}", items);
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
    private ItemReader<DrivingRecord> csvMvmnItemReader() throws Exception {
        DefaultLineMapper<DrivingRecord> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
//        tokenizer.setNames("carId", "stateOfChrgBms", "stateOfChrgDisp", "rapidChrgPort", "normalChrgPort",
//                "stateOfHealth", "btryMinTempr", "btryMaxTempr", "minCellVolt", "maxCellVolt", "btryCellsArr", "btryMdulTemprArr","mvmnTime");
        lineMapper.setLineTokenizer(tokenizer);

        lineMapper.setFieldSetMapper(fieldSet -> {
            log.info("fieldSet, {}", fieldSet);
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

        FlatFileItemReader itemReader = new FlatFileItemReaderBuilder<DrivingRecord>()
                .name("csvMvmnItemReader")
                .encoding("UTF-8")
                .resource(new ClassPathResource("drivingRecord.csv"))
//                .linesToSkip(1)
                .lineMapper(lineMapper)
                .build();
        itemReader.afterPropertiesSet();

        return itemReader;
    }

    private ItemProcessor<DrivingRecord, DrivingRecord> mvmnItemProcessor() {
        return item -> {
            Thread.sleep(1000);

            item.setUpdatedTime(LocalDateTime.now());
            log.info("processor: {}", item);
            return item;
        };
    }

    private ItemWriter<DrivingRecord> jdbcBatchMvmnItemWriter() {

        JdbcBatchItemWriter<DrivingRecord> itemWriter = new JdbcBatchItemWriterBuilder<DrivingRecord>()
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("insert into driving_record(car_id, charge_status, key_status, " +
                        "odometer, soc_display , soc_real, updated_time, " +
                        "vehicle_speed , xcord, ycord) " +
                        "values (:carId, :chargeStatus, :keyStatus, :odometer,:socDisplay, :socReal,  " +
                        ":updatedTime, :vehicleSpeed, :xcord, :ycord)")
                .build();
        itemWriter.afterPropertiesSet();

        return itemWriter;
    }

}
