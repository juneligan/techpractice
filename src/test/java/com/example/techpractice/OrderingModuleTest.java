package com.example.techpractice;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.example.techpractice.TestUtil.createMessages;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OrderingModuleTest {

    @Test
    void onMessageReceivedTest() {
        // given
        OrderingModule orderingModule = new OrderingModule(2, 1, false);
        Message expected = new Message(1, 1, "expected");

        // when
        orderingModule.onMessageReceived(expected);
        orderingModule.run();

        // then
        assertEquals(expected, orderingModule.getOutputQueue().poll());
    }

    @Test
    void getOutputQueueTest() {
        // given
        OrderingModule orderingModule = new OrderingModule(2, 1, false);
        Message expected = new Message(1, 1, "expected");
        orderingModule.onMessageReceived(expected);

        orderingModule.run();

        // when
        Message actual = orderingModule.getOutputQueue().poll();

        // then
        assertEquals(expected, actual);
    }

    @Test
    void runTestFirstBatchOutputQueue() {
        assertRunMethodResults(
                10,
                Arrays.asList(4, 8, 12, 3, 7, 11, 1, 2, 15, 5), // expected ordered ids
                om -> { // message consumer Function
                    Queue<Message> outputQueue = om.getOutputQueue();
                    int outSize = outputQueue.size();
                    return IntStream.range(0, outSize).mapToObj(o -> outputQueue.poll()).toList();
                }
        );
    }

    @Test
    void runTestRemainingBatchOutputQueue() {
        // given
        assertRunMethodResults(
                10,
                Arrays.asList(9, 6, 13, 10, 14), // expected ordered ids
                om -> {
                    Queue<Message> outputQueue1 = om.getOutputQueue();
                    int outSize1 = outputQueue1.size();
                    IntStream.range(0, outSize1).forEach(o -> outputQueue1.poll());

                    IntStream.range(0, 10).forEach(o -> om.run());

                    Queue<Message> outputQueue2 = om.getOutputQueue();
                    int outSize2 = outputQueue2.size();
                    return IntStream.range(0, outSize2).mapToObj(o -> outputQueue2.poll()).toList();
                }
        );
    }

    @Test
    void runTestWhenMessagesIsLessThanOutputQueueLimit() {
        // given
        assertRunMethodResults(
                20,
                Arrays.asList(4, 8, 12, 3, 7, 11, 1, 2, 15, 5, 6, 9, 10, 13, 14), // expected ordered ids
                om -> {
                    Queue<Message> outputQueue = om.getOutputQueue();
                    int outSize = outputQueue.size();
                    return IntStream.range(0, outSize).mapToObj(o -> outputQueue.poll()).toList();
                }
        );
    }

    private static void assertRunMethodResults(
            int outputQueueLimit,
            List<Integer> ids,
            Function<OrderingModule, List<Message>> consumingFunction
    ) {
        // given
        OrderingModule orderingModule = new OrderingModule(outputQueueLimit, 50, false);

        List<Message> messages = createMessages(15, 4);
        List<Message> expected = ids.stream()
                .map(o -> messages.get(o - 1))
                .collect(Collectors.toList());

        messages.forEach(orderingModule::onMessageReceived);

        // when
        IntStream.range(0, outputQueueLimit).forEach(o -> orderingModule.run());
        List<Message> actual = consumingFunction.apply(orderingModule);

        // then
        assertEquals(expected, actual);
    }
}
// NOTES
// ID  - channel ID
//    1-1
//    2-2
//    3-3
//    4-4
//    5-1
//    6-2
//    7-3
//    8-4
//    9-1
//    10-2
//    11-3
//    12-4
//    13-1
//    14-2
//    15-3

// expected sorted by channel id that is limited by transfer size
//    4-4
//    8-4
//    12-4
//    3-3
//    7-3
//    11-3
//    1-1
//    2-2
//    15-3
//    5-1
//    6-2
//    9-1
//    10-2

// Grouping by 4
//    4-4
//    8-4
//    12-4
//    3-3

//    7-3
//    11-3
//    15-3
//    1-1

//    5-1
//    2-2
//    9-1
//    6-2
//    10-2