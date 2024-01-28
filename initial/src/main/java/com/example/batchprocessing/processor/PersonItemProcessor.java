package com.example.batchprocessing.processor;

import com.example.batchprocessing.domain.Person;
import org.springframework.batch.item.ItemProcessor;

/**
 * ItemProcessor: 중간 프로세서 reader에게 받은 데이터를 writer에 넘기기 전에 전처리 진행, 데이터를 가공하거나 필터링하는 역할 reader,
 * writer와 단계 분리 -> 비즈니스 코드가 섞이는 것 방지 데이터를 개별건으로 가공하거나 필터링(writer로 넘길지 말지)한다. => chunkSize 단위로 묶지
 * 않는다!
 * <p>
 * 함수형 인터페이스이기 때문에 굳이 구현체를 두지 않고, 람다식으로 대체할 수도 있다. chunk 만큼 작업을 완료하면, writer에 전달한다.
 */
public class PersonItemProcessor implements ItemProcessor<Person, Person> {

    // ItemProcessor<I, O> -> I: ItemReader에서 받을 데이터 타입, O: ItemWriter로 보낼 데이터 타입
    @Override
    public Person process(Person person) throws Exception {
        final String upperFirstName = person.firstName().toUpperCase();
        final String upperLastName = person.lastName().toUpperCase();

        System.out.println("convert to upperString");

        // null을 반환하면 writer로 해당 데이터를 넘기지 않는다. => 필터링
        return new Person(upperFirstName, upperLastName);
    }
}
