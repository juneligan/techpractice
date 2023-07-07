package com.example.techpractice;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageConsumerTest {

    @Mock
    private OrderedMessageQueueProvider mockedOrderedMessageQueueProvider;

    @Mock
    private java.util.Queue<Message> mockedQueue;

    private MessageConsumer messageConsumer;

    @BeforeEach
    void setup() {
        messageConsumer = new MessageConsumer(mockedOrderedMessageQueueProvider);
    }

    @Test
    void run() {
        // given
        Message message = new Message(1, 1, "1");
        when(mockedOrderedMessageQueueProvider.getOutputQueue()).thenReturn(mockedQueue);
        when(mockedQueue.size()).thenReturn(1);
        when(mockedQueue.poll()).thenReturn(message);

        // when
        messageConsumer.run();

        // then
        verify(mockedOrderedMessageQueueProvider).getOutputQueue();
        verify(mockedQueue, times(2)).size();
        verify(mockedQueue, times(2)).poll();
    }
}