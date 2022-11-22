package com.snort.intelli.app.config;


import com.snort.intelli.app.entites.Todos;
import com.snort.intelli.app.listener.TodosListener;
import com.snort.intelli.app.repository.TodosRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import java.util.List;
import java.util.function.Function;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    //Bean

    @Bean
    public FlatFileItemReader<Todos> reader(){
        //FlatFileItem : CSV file
        FlatFileItemReader<Todos> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("CSV-Todos-with-Header-Data-File.csv"));
        reader.setLinesToSkip(1);
        reader.setLineMapper(
                new DefaultLineMapper(){{
                    setLineTokenizer(new DelimitedLineTokenizer(){{
                        setDelimiter(DELIMITER_COMMA);
                    setNames("taskId","title","description","completed","assignedDate","updatedDate"); }});
                    setFieldSetMapper(
                            new BeanWrapperFieldSetMapper(){{
                                setTargetType(Todos.class);
                            }});
                }});
        reader.setRecordSeparatorPolicy(new BlankLineRecordSeparatorPolicy());
        return reader;
    }//end of reader()

    @Autowired
    private TodosRepository todosRepository;

    @Bean
    public ItemWriter<Todos> writer(){
        return todosList->{
            //write data where ? MySQL Db
            todosRepository.saveAll(todosList);
        };
    }//end of ItemWriter

    @Bean
    public ItemProcessor<Todos, Todos> todosItemProcessor(){
        //if you want to modify your data then use it
        return eachTodos -> {
            String description = eachTodos.getDescription().toUpperCase();
            eachTodos.setDescription(description);
            return eachTodos;
        };
    }//end of ItemProcessor

    @Bean
    public JobExecutionListener jobExecutionListener(){
        return new TodosListener();
    }//end of JobExecutionListener


    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Step stepA(){
        return stepBuilderFactory.
                get("stepA").<Todos, Todos>
                        chunk(100).
                reader(reader())
                .processor(todosItemProcessor())
                .writer(writer()).build();
    }

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Bean
    public Job JobA(){
       return jobBuilderFactory.get("JobA").incrementer(new RunIdIncrementer()).
                listener(jobExecutionListener()).start(stepA()).build();
    }


}//end of class
