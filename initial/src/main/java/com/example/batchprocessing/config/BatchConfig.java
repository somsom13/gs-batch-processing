package com.example.batchprocessing.config;

import com.example.batchprocessing.domain.Person;
import com.example.batchprocessing.processor.JobCompletionNotificationListener;
import com.example.batchprocessing.processor.PersonItemProcessor;
import javax.sql.DataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

@Configuration
public class BatchConfig {

    // bean을 추가하여 reader, processor, writer 정의
    @Bean
    public FlatFileItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>() // reader를 만드는 빌더
            .name("personItemReader")
            .resource(new ClassPathResource("sample-data.csv")) // input data를 어디에서 받아올지
            .delimited() // 한 라인에서 구분자를 무엇으로 할지: 기본자가 ,(comma) -> delimited().delimiter(구분자)
            .names("firstName", "lastName") // 파일 내에서 구분된 데이터의 컬럼명 지정
            .targetType(Person.class) // 읽은 데이터를 변환할 데이터 타입(dto) 지정
            .build();
    }

    // 중간 전처리기
    @Bean
    public PersonItemProcessor middleProcessor() {
        return new PersonItemProcessor();
    }

    // ItemWriter: spring batch에서 사용하는 출력 기능
    // Chunk 기반 처리로 인해 item을 chunk단위로 묶은 item list를 다룬다.
    // JdbcItemWriter: JDBC의 batch 기능을 사용해 한 번에 DB로 데이터를 전달 -> db 내부에서 쿼리 실행
    // 즉, chunkSize만큼 쿼리를 쌓아뒀다가 한 번에 쿼리를 전송, 실행되게 하는 것
    // ** JdbcBatchItermWriter의 제네릭 타입은 reader가 넘겨주는 데이터 타입이다!!!
    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
            .sql(
                "INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)"
            ) // dto의 getter, Map의 key에 따라 파라미터 바인딩
            .dataSource(dataSource)
            .beanMapped() // pojo 기반으로 insert sql의 value를 매핑한다. -> reader에서 writer로 넘겨주는 타입이 pojo인 경우
            // 만약 reader가 넘겨주는  데이터 타입이 Map<String, Object> 라면 columnMapped 를 사용한다.
            .build();
    }

    // job 정의
    @Bean
    public Job importUserJob(
        JobRepository jobRepository,
        Step step1, // configuration으로 정의한 step -> 해당 job을 구성한다.
        JobCompletionNotificationListener listener
    ) { // job이 모두 완료되었음을 수신하는 listener
        return new JobBuilder("importUserJob", jobRepository)
            .listener(listener)
            .start(step1)
            .build();
    }

    // 단일 step 정의 -> job은 step들로 구성된다.
    // 각 step은 reader, processor, writer 로 구성될 수 있다.
    // step을 정의하면서 한 번에 데이터를 얼마나 write 할지 정할 수 있다.
    @Bean
    public Step step1(
        JobRepository jobRepository,
        DataSourceTransactionManager transactionManager,
        FlatFileItemReader<Person> reader,
        PersonItemProcessor processor,
        JdbcBatchItemWriter<Person> writer
    ) {
        return new StepBuilder("step1", jobRepository)
            .<Person, Person>chunk(3, transactionManager)
            // chunk는 generic 메서드이기 때문에, 앞에 <I, O> prefix를 가진다.
            // chunkSize를 3으로 설정 => 한 번에 3개의 데이터를 write 한다.
            .reader(reader)
            .processor(processor)
            .writer(writer) // 해당 step에서 사용할 reader, writer, processor 를 정의한다.
            .build();
    }

}
