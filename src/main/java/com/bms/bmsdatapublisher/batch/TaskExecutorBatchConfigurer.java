package com.bms.bmsdatapublisher.batch;

import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
@Configuration
public class TaskExecutorBatchConfigurer extends DefaultBatchConfigurer {

    @Bean
    public ThreadPoolTaskScheduler batchTaskScheduler() {
        ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setPoolSize(10);
        threadPoolTaskScheduler.afterPropertiesSet();
        return threadPoolTaskScheduler;
    }


    @Override
    protected JobLauncher createJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(super.getJobRepository());
        jobLauncher.setTaskExecutor(batchTaskScheduler());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }
}
